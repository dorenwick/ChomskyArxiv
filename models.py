from dataclasses import dataclass


@dataclass
class ModelPaths:
    """
    Configuration for model paths.
    """
    BASE_DIR = "E:/CiteGrabberFinal"
    MODELS_DIR = f"{BASE_DIR}/Models"
    VECTOR_DIR = f"{BASE_DIR}/VectorIndex"

    FIELD_CLASSIFIER = f"{MODELS_DIR}/cite-grabber-field-classifier-small"
    SUBFIELD_CLASSIFIER = f"{MODELS_DIR}/cite-grabber-subfield-classifier-small"
    NER_MODEL = f"{MODELS_DIR}/cite--grabber--ner--small"
    RELATION_EXTRACTOR = f"{MODELS_DIR}/cite-grabber-relation-extractor-small"
    SENTENCE_ENCODER = "Snowflake/snowflake-arctic-embed-xs"

    VECTOR_INDEX = f"{VECTOR_DIR}/full_index.faiss"

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("Snowflake/snowflake-arctic-embed-xs")

    sentences = [
        "That is a happy person",
        "That is a happy dog",
        "That is a very happy person",
        "Today is a sunny day"
    ]
    embeddings = model.encode(sentences)

    similarities = model.similarity(embeddings, embeddings)
    print(similarities.shape)