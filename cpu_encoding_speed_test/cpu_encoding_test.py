
from sentence_transformers import SentenceTransformer
import torch
import time
import numpy as np
from tqdm import tqdm


def run_benchmark(device: str, iterations: int = 10000, batch_size: int = 32):
    print(f"\nRunning benchmark on {device.upper()}")

    # Load model on specified device
    model = SentenceTransformer("Snowflake/snowflake-arctic-embed-xs")

    if device == 'cpu':
        model = model.cpu()
        torch.set_num_threads(16)  # Optional: control CPU threads
    else:
        model = model.cuda()

    print(f"Model loaded on device: {next(model.parameters()).device}")
    if device == 'cuda':
        print(f"GPU Info: {torch.cuda.get_device_name(0)}")
        print(f"CUDA Version: {torch.version.cuda}")

    # Test sentences
    single_sentences = [
        "Artificial intelligence has revolutionized modern technology.",
        "The quantum mechanics principles underpin our understanding of physics.",
        "Climate change poses significant challenges to global ecosystems.",
        "Neural networks have enabled breakthrough advances in computer vision.",
        "The human genome project has transformed medical research."
    ]

    # Create batched sentences
    batched_sentences = single_sentences * (batch_size // len(single_sentences))
    if len(batched_sentences) < batch_size:
        batched_sentences.extend(single_sentences[:(batch_size - len(batched_sentences))])

    # Store timings for each approach
    single_timings = []
    batch_timings = []

    # Run single sentence benchmark
    print("\nRunning single sentence encoding...")
    for _ in tqdm(range(iterations), desc=f"{device.upper()} Single Encoding"):
        start_time = time.perf_counter()
        embeddings = model.encode(single_sentences)
        end_time = time.perf_counter()
        single_timings.append(end_time - start_time)

    # Run batch encoding benchmark
    print("\nRunning batch encoding...")
    for _ in tqdm(range(iterations), desc=f"{device.upper()} Batch Encoding"):
        start_time = time.perf_counter()
        embeddings = model.encode(batched_sentences)
        end_time = time.perf_counter()
        batch_timings.append(end_time - start_time)

    # Calculate statistics for single encoding
    single_avg = np.mean(single_timings)
    single_std = np.std(single_timings)
    single_min = np.min(single_timings)
    single_max = np.max(single_timings)
    single_per_sentence = single_avg / len(single_sentences)

    # Calculate statistics for batch encoding
    batch_avg = np.mean(batch_timings)
    batch_std = np.std(batch_timings)
    batch_min = np.min(batch_timings)
    batch_max = np.max(batch_timings)
    batch_per_sentence = batch_avg / len(batched_sentences)

    print(f"\n{device.upper()} Single Encoding Statistics:")
    print(f"Average time: {single_avg:.4f} seconds")
    print(f"Standard deviation: {single_std:.4f} seconds")
    print(f"Min time: {single_min:.4f} seconds")
    print(f"Max time: {single_max:.4f} seconds")
    print(f"Time per sentence: {single_per_sentence:.4f} seconds")

    print(f"\n{device.upper()} Batch Encoding Statistics (batch_size={batch_size}):")
    print(f"Average time: {batch_avg:.4f} seconds")
    print(f"Standard deviation: {batch_std:.4f} seconds")
    print(f"Min time: {batch_min:.4f} seconds")
    print(f"Max time: {batch_max:.4f} seconds")
    print(f"Time per sentence: {batch_per_sentence:.4f} seconds")

    return single_avg, batch_avg, single_per_sentence, batch_per_sentence


def main():
    iterations = 1000
    batch_size = 32
    print("Starting encoding benchmark...")
    print(f"Model: Snowflake/snowflake-arctic-embed-xs")
    print(f"Iterations: {iterations}")
    print(f"Batch size: {batch_size}")
    print(
        f"Total encodings per device: {iterations * 5} sentences (single) + {iterations * batch_size} sentences (batched)")

    # Run CPU benchmark
    cpu_single_time, cpu_batch_time, cpu_single_per, cpu_batch_per = run_benchmark('cpu', iterations, batch_size)

    # Run GPU benchmark if available
    if torch.cuda.is_available():
        gpu_single_time, gpu_batch_time, gpu_single_per, gpu_batch_per = run_benchmark('cuda', iterations, batch_size)

        # Compare results
        print("\nPerformance Comparisons:")
        print(f"CPU batch vs single speedup: {cpu_single_time / cpu_batch_time:.2f}x")
        print(f"GPU batch vs single speedup: {gpu_single_time / gpu_batch_time:.2f}x")
        print(f"GPU vs CPU (single) speedup: {cpu_single_time / gpu_single_time:.2f}x")
        print(f"GPU vs CPU (batch) speedup: {cpu_batch_time / gpu_batch_time:.2f}x")

        print("\nPer-sentence Performance:")
        print(f"CPU single: {cpu_single_per:.4f} seconds")
        print(f"CPU batch: {cpu_batch_per:.4f} seconds")
        print(f"GPU single: {gpu_single_per:.4f} seconds")
        print(f"GPU batch: {gpu_batch_per:.4f} seconds")
    else:
        print("\nGPU not available for testing")
        print("\nCPU Performance:")
        print(f"Batch vs single speedup: {cpu_single_time / cpu_batch_time:.2f}x")


if __name__ == "__main__":
    main()
