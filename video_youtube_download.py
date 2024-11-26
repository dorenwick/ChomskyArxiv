import time
from pathlib import Path

import yt_dlp
from tqdm import tqdm


class VideoDownloader:
    def __init__(self):
        self.base_dir = Path("C:/Users/doren/PycharmProjects/ChomskyArchive/web_data")
        self.videos_dir = self.base_dir / "videos"
        self.videos_dir.mkdir(exist_ok=True)

        # Configure yt-dlp options
        self.ydl_opts = {
            'format': 'best',  # Changed to download best available single format
            'paths': {'home': str(self.videos_dir)},
            'outtmpl': {'default': '%(title)s.%(ext)s'},
            'quiet': False,
            'no_warnings': False,
            'progress': True,
            'prefer_ffmpeg': False,  # Don't require ffmpeg
            'postprocessors': []  # No post-processing needed
        }

    def download_video(self, url: str) -> bool:
        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                print(f"\nDownloading: {url}")
                info = ydl.extract_info(url, download=True)
                print(f"Successfully downloaded: {info.get('title', 'Unknown title')}")
                return True

        except Exception as e:
            print(f"Error downloading {url}: {str(e)}")
            return False
        finally:
            # Add delay to prevent rate limiting
            time.sleep(1)


def main():
    downloader = VideoDownloader()

    urls = [
        "https://www.youtube.com/watch?v=eRLLRzmbk6g",
        "https://www.youtube.com/watch?v=8aDjX54nmJY",
        "https://www.youtube.com/watch?v=Sk2pVd9Wdiw",
        "https://www.youtube.com/watch?v=T-IZY0VUQ0s",
        "https://www.youtube.com/watch?v=TQ-Crh3rdQA",
        "https://www.youtube.com/watch?v=tJGYmfTaFRw",
        "https://www.youtube.com/watch?v=T7euc5WZbCw",
        "https://www.youtube.com/watch?v=RiA9PtTLi-Q",
        "https://www.youtube.com/watch?v=XgBnPwklg6U",
        "https://www.youtube.com/watch?v=nIQ2WaZ2R2Y",
        "https://www.youtube.com/watch?v=IgxzcOugvEI",
        "https://www.youtube.com/watch?v=D_1E8BTSVqM",
        "https://www.youtube.com/watch?v=yvUTXdpjA3w",
        "https://www.youtube.com/watch?v=3vFX0-EWLSo",
        "https://www.youtube.com/watch?v=0bGQoR2BF74",
        "https://www.youtube.com/watch?v=MJgd5vzU4QU",
        "https://www.youtube.com/watch?v=bdRLyshuWQg",
        "https://www.youtube.com/watch?v=O7Vueji6LQo",
        "https://www.youtube.com/watch?v=QhyRJf-Faas",
        "https://www.youtube.com/watch?v=g2Vx5Ze_p8s",
        "https://www.youtube.com/watch?v=AP3_e1ohpkQ",
        "https://www.youtube.com/watch?v=7uHGlfeCBbE",
        "https://www.youtube.com/watch?v=RN_ZfJAXHeU",
        "https://www.youtube.com/watch?v=-0AAyMcyYlg",
        "https://www.youtube.com/watch?v=867fCDT4paM",
        "https://www.youtube.com/watch?v=saE-OQlXtno",
        "https://www.youtube.com/watch?v=740kSHH7VSY",
        "https://www.youtube.com/watch?v=bMNeQFkBcjw",
        "https://www.youtube.com/watch?v=jIv-glBvrrc",
        "https://www.youtube.com/watch?v=TYCt8lv7f9o",
        "https://www.youtube.com/watch?v=pYla4M0OU3Y",
        "https://www.youtube.com/watch?v=QMY9-zJJm-M",
        "https://www.youtube.com/watch?v=UYDkmZAbTx0",
        "https://www.youtube.com/watch?v=8Jr0PCU4m7M",
        "https://www.youtube.com/watch?v=Fb7AD49WIlY",
        "https://www.youtube.com/watch?v=eQo3Fnf8aJ8",
        "https://www.youtube.com/watch?v=68Rh1tKx98k",
        "https://www.youtube.com/watch?v=e8uUeuvMQYU",
        "https://www.youtube.com/watch?v=hRrPBvWQmq4",
        "https://www.youtube.com/watch?v=ML3XIjH-l_s",
        "https://www.youtube.com/watch?v=vxwd51WK3Hc",
        "https://www.youtube.com/watch?v=T4fuzd1SlOc",
        "https://www.youtube.com/watch?v=9z-YbPgfgfw",
        "https://www.youtube.com/watch?v=Y0Jqa_KAdN8",
        "https://www.youtube.com/watch?v=-P_pJ6rFhbM",
        "https://www.youtube.com/watch?v=HXAe9qYDxh4",
        "https://www.youtube.com/watch?v=VGFdvw8cOR4",
        "https://www.youtube.com/watch?v=b6M9VxHLqV8",
        "https://www.youtube.com/watch?v=6MsLADSQvjQ",
        "https://www.youtube.com/watch?v=rv18HU4CfXQ",
        "https://youtu.be/pJRAMR8tGkQ",
        "https://www.youtube.com/watch?v=uwznEEtcBls",
        "https://www.youtube.com/watch?v=Up4qJGtjukw",
        "https://youtu.be/h8gXe6fejew",
        "https://www.youtube.com/watch?v=TeiAFCSN-kQ",
        "https://www.youtube.com/watch?v=cB79IEuyfx4",
        "https://www.youtube.com/watch?v=d6jpniwteYE",
        "https://www.youtube.com/watch?v=kSxAMBJor2U",
        "https://www.youtube.com/watch?v=9b62321CWZ0",
        "https://www.youtube.com/watch?v=adLkhHVPPxo",
        "https://www.youtube.com/watch?v=xT7sOMYB1Ko",
        "https://www.youtube.com/watch?v=OS8qzcGDP5U&feature=youtu.be",
        "https://www.youtube.com/watch?v=Huy82PVaCzs",
        "https://www.youtube.com/watch?v=tGsUh3dOF0c",
        "https://www.youtube.com/watch?v=Zs-k1npk0Q8&feature=youtu.be",
        "https://www.youtube.com/watch?v=6Nf5TFpR9Rk",
        "https://www.youtube.com/watch?v=mySzyrrA4P4",
        "https://www.youtube.com/watch?v=8QpUH-35n9c&feature=youtu.be",
        "https://www.youtube.com/watch?v=e_0DAud74mY",
        "https://www.youtube.com/watch?v=2fojpMEo1Oo",
        "https://www.youtube.com/watch?v=0FawIkrCey0",
        "https://www.youtube.com/watch?v=mobXf7Pnrjc&feature=emb_title",
        "https://www.youtube.com/watch?v=uf9CAGDaK_o",
        "https://www.youtube.com/watch?v=IqfWUe0UbZA",
        "https://www.youtube.com/watch?v=n_LD6alcsn0&feature=youtu.be",
        "https://www.youtube.com/watch?v=SPg0fEbObtA&feature=emb_logo",
        "https://www.youtube.com/watch?v=VUYYZdE5Dmk",
        "https://www.youtube.com/watch?v=Vq6IgBMg3lI&feature=emb_title",
        "https://www.youtube.com/watch?v=KjvkDnPogVY&feature=youtu.be",
        "https://www.youtube.com/watch?v=t-N3In2rLI4&feature=youtu.be",
        "https://www.youtube.com/watch?v=p4PBVPbL_Ak&feature=youtu.be",
        "https://www.youtube.com/watch?v=2tWgc1CLlBI&feature=youtu.be",
        "https://www.youtube.com/watch?v=yuVqfKYbGvE&feature=emb_title",
        "https://www.youtube.com/watch?v=4plIX68mdE8",
        "https://www.youtube.com/watch?v=X7nzyrbWv9E",
        "https://www.youtube.com/watch?v=pf-tQYcZGM4&feature=youtu.be",
        "https://www.youtube.com/watch?v=H_gebfB4c6k&feature=youtu.be",
        "https://www.youtube.com/watch?v=jfXbgNOvMN0&feature=youtu.be",
        "https://youtu.be/hnocpl68zjU",
        "https://www.youtube.com/watch?v=YFKKxWQz7R0",
        "https://www.youtube.com/watch?v=8ZVpin57aWQ&feature=share",
        "https://www.youtube.com/watch?v=mTFPZbRZXDU&feature=youtu.be",
        "https://youtu.be/rzu2UcIxsT4",
        "https://www.youtube.com/watch?v=o4oMS3rU9d0&feature=emb_title",
        "https://www.youtube.com/watch?v=2C-zWrhFqpM&feature=youtu.be",
        "https://youtu.be/PbjtRgYhLzU",
        "https://www.youtube.com/watch?v=uXHwyuS3TiA",
        "https://www.youtube.com/watch?v=Z5A8BVrjRfU&feature=youtu.be",
        "https://www.youtube.com/watch?v=71CWfb4sAmE",
        "https://www.youtube.com/watch?v=jB54XxbgI0E&feature=youtu.be",
        "https://www.youtube.com/watch?v=BJCST5akQQo",
        "https://www.youtube.com/watch?v=fd61HhrckFQ&feature=emb_title",
        "https://www.youtube.com/watch?v=yD1KuGILOaE",
        "https://www.youtube.com/watch?v=cka5Sozfq0k",
        "https://www.youtube.com/watch?v=gxLa6jtF01g",
        "https://www.youtube.com/watch?v=fCzqb37DfhA",
        "https://www.youtube.com/watch?v=EbYMoUa9_BU",
        "https://youtu.be/P2lsEVlqts0?list=PLXjqQf1xYLQ4b5OjpAFJaErGcxTB8xI6l",
        "https://www.youtube.com/watch?v=kEWmtiKjpTs",
        "https://www.youtube.com/watch?v=9O09PViLGGs",
        "https://www.youtube.com/watch?v=9xV_Iu4QylY",
        "https://www.youtube.com/watch?v=iJaR7oEfy8I",
        "https://www.youtube.com/watch?v=ytex5sq_YiA",
        "https://www.youtube.com/watch?v=Q3bAfdE4Mdg",
        "https://www.youtube.com/watch?v=Q3bAfdE4Mdg",
        "https://www.youtube.com/watch?v=bENkcPi30zU",
        "https://www.youtube.com/watch?v=XNSxj0TVeJs",
        "https://youtu.be/bwAWGrCWwK0",
        "https://www.youtube.com/watch?v=1w1kjMXHv6o",
        "https://www.youtube.com/watch?v=0XnihgXhh3c",
        "https://www.youtube.com/watch?v=t0jHFpTJQSw",
        "https://www.youtube.com/watch?v=i63_kAw3WmE",
        "https://www.youtube.com/watch?v=yS7zD8nnjsg",
        "https://www.youtube.com/watch?v=mBZLnfKSa_k&list=PLNAlnQ4hvLtTAJcIEcfvfHbMv2omP_rHC",
        "https://www.youtube.com/watch?v=w_X5czMVKT8",
        "https://www.youtube.com/watch?v=kA7pzbjySr4",
        "https://www.youtube.com/watch?v=JFiCg67cDTs&feature=youtu.be",
        "https://www.youtube.com/watch?v=4-X8vxzjY2A",
        "https://www.youtube.com/watch?v=CvZRsdHgxgA",
        "https://www.youtube.com/watch?v=ckwDj0ByqSs",
        "https://youtu.be/ntxToEBCQb4?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/R0JqIdNvIg4?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://www.youtube.com/watch?v=ZyUX7e7g_Zs",
        "https://www.youtube.com/watch?v=rGHpWST7l9I",
        "https://www.youtube.com/watch?v=2HtZV_jU4sI",
        "https://www.youtube.com/watch?v=Ws1wHI2k-C8",
        "https://www.youtube.com/watch?v=x2dw7OZD-mg",
        "https://www.youtube.com/watch?v=Ml1G919Bts0",
        "https://www.youtube.com/watch?v=tbxp8ViBTu8",
        "https://www.youtube.com/watch?v=PEdScUEf4y0",
        "https://www.youtube.com/watch?v=UgxLvwKhvmY",
        "https://www.youtube.com/watch?list=PLZNV4DrfcAmHk88zdCZJ4nydI8NurqcvL&v=ZLBXIujfMvE",
        "https://www.youtube.com/watch?v=aBJeBu0lBKQ",
        "https://www.youtube.com/watch?v=YmshN5b92Ic",
        "https://www.youtube.com/watch?v=9JVVRWBekYo",
        "https://www.youtube.com/watch?v=W_W7_m1ljJM&feature=youtu.be",
        "https://www.youtube.com/watch?v=zk3W3bEFw5U&feature=youtu.be",
        "https://www.youtube.com/watch?v=kdM9wHU2w4c&feature=youtu.be",
        "http://www.youtube.com/watch?v=TTnSl79cjX0",
        "https://www.youtube.com/watch?v=5meC4Z61qGg",
        "https://www.youtube.com/watch?v=HHucMFKM44Q",
        "https://youtu.be/3jILYiuISsQ?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/Qzi3PkTZchE?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://www.youtube.com/watch?v=kfbOlh50hM0",
        "https://www.youtube.com/watch?v=kfbOlh50hM0",
        "https://www.youtube.com/watch?v=_FHNMZbnvYU",
        "http://www.youtube.com/watch?v=rNkXaxd9Dow",
        "http://www.youtube.com/watch?v=1eGlgOnHOJE",
        "https://www.youtube.com/watch?v=NJh5muRa8zk",
        "https://www.youtube.com/watch?v=o9eRrSfNzng",
        "https://youtu.be/9uNYsbOKIFw?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/8mxp_wgFWQo?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/deo4W3NIYEI?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/XTQD6qeCb_0?list=PLTiEffrOcz_4YIz1QvotlAPz9s8GwYa5F",
        "https://www.youtube.com/watch?v=K_Z9bsIsANw",
        "https://www.youtube.com/watch?v=Av8uFvDTvw4",
        "http://www.youtube.com/watch?v=XryhafGJ1_Y",
        "http://www.youtube.com/watch?v=wJtfWZGxnGI",
        "http://www.youtube.com/watch?v=zdx2CA7OkZ0",
        "https://youtu.be/UEpn68BZIOY?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/tbVhz5SUmtw?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "http://www.youtube.com/watch?v=NY8ig9n9EFY",
        "https://www.youtube.com/watch?v=Y3PwG4UoJ0Y",
        "https://www.youtube.com/watch?v=L8gzcGKbYZ8",
        "http://www.youtube.com/watch?v=SylqeewUB1Y",
        "http://www.youtube.com/watch?v=MautscPF5wE&",
        "https://www.youtube.com/watch?v=UFnQrOi-9T0",
        "http://www.youtube.com/watch?v=oB9rp_SAp2U",
        "https://youtu.be/hCYkD15SK7M?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "http://www.youtube.com/watch?v=zLPYN82xHck",
        "https://www.youtube.com/watch?v=lcb_iN2RhpM",
        "https://www.youtube.com/watch?v=n9uD4bkx-JM",
        "http://www.youtube.com/watch?v=0kICLG4Zg8s",
        "https://www.youtube.com/watch?v=UCZp8sEpdq0",
        "https://www.youtube.com/watch?v=708pvp0E9dI",
        "https://www.youtube.com/watch?v=jNTFHpLDT1U",
        "https://www.youtube.com/watch?feature=player_embedded&v=tlONaMmIT1o",
        "https://www.youtube.com/watch?v=6HPVtTjv6gk",
        "http://www.youtube.com/watch?v=2Ll6M0cXV54",
        "https://www.youtube.com/watch?v=-tRBsCVZpP4",
        "https://www.youtube.com/watch?v=iR_NmkkMmO8",
        "https://www.youtube.com/watch?feature=player_embedded&v=p1QRnErqV3U",
        "https://www.youtube.com/watch?v=urrNTVxuCxs",
        "http://www.youtube.com/watch?v=QrT5xtDSDBo",
        "https://youtu.be/7TLZN92-dZo?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/wgHqwqoQvVg?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "http://www.youtube.com/watch?v=FYbevorJY7c",
        "http://www.youtube.com/watch?v=aDx2-mdInhI",
        "https://www.youtube.com/watch?v=o63EjNGGe3A",
        "https://www.youtube.com/watch?v=8FggxAtyxN8",
        "https://www.youtube.com/watch?v=Rgd8BnZ2-iw",
        "https://youtu.be/Bl4tm0lHdIQ?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/zEUn2cYkzKc?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/6JbnuoD2v50?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://youtu.be/Grn2EZxcUeA?list=PLOldj3YIjHZ7Qt6uzeCuV8O_fI713fljB",
        "https://www.youtube.com/watch?v=_9CHtm2qK2g",
        "https://www.youtube.com/watch?v=8BK0XIm0DXE",
        "https://www.youtube.com/watch?v=xusFPZHuZvE",
        "https://www.youtube.com/watch?v=D5in5EdjhD0",
        "https://www.youtube.com/watch?v=laMs3iFRP80",
        "https://www.youtube.com/watch?v=e_EgdShO1K8",
        "https://www.youtube.com/watch?v=Q-JX0yXDlh8",
        "http://www.youtube.com/watch?v=csE-MsT_NN0&feature=plcp&context=C38b34ecUDOEgsToPDskJgzQvWjWW2GYwFBHchoh6X",
        "http://www.youtube.com/watch?v=hMo77yaqQrc",
        "http://www.youtube.com/watch?v=W5hz8JYZNeU",
        "http://youtu.be/dCH_qhC5RRc",
        "http://www.youtube.com/watch?v=F5zCqHnd_pY",
        "http://www.youtube.com/watch?v=oWUfw9Db2dE",
        "http://www.youtube.com/watch?v=pbxLA2uTWuw",
        "https://www.youtube.com/watch?v=4sY4WIyCcN0",
        "http://www.youtube.com/watch?v=5sT1Ud3xqCQ",
        "http://www.youtube.com/watch?v=oWvf4sEZ_ls",
        "http://www.youtube.com/watch?v=tmJshykd3WQ",
        "https://www.youtube.com/watch?v=6i_W6Afed2k",
        "http://www.youtube.com/watch?v=E9s7JuEceNI",
        "http://www.youtube.com/watch?v=dBZ-NvbZWH8&",
        "http://www.youtube.com/watch?v=2v6XFkSwVys&",
        "http://www.youtube.com/user/uoftscarborough#p/c/0/Q97tFyqHVLs",
        "http://www.youtube.com/watch?v=QOudyiO2384",
        "http://www.youtube.com/watch?v=tyQ45fEkaUY",
        "http://www.youtube.com/watch?v=c45_Dib6ODY",
        "http://www.youtube.com/watch?v=KAzEzPzXopQ",
        "http://www.youtube.com/watch?v=PCbomxeeLco",
        "http://www.youtube.com/watch?v=GcIVNzcMucU",
        "http://www.youtube.com/watch?v=oiwAFIgGCkQ",
        "http://www.youtube.com/watch?v=_O3cNc2JoMA",
        "http://www.youtube.com/watch?v=z-VtG6sUlJc",
        "http://www.youtube.com/watch?v=8jiWU4uwZa0",
        "http://www.youtube.com/watch?v=Zdvgg9D5JZE",
        "http://www.youtube.com/watch?v=LTVgomRM1Go",
        "http://www.youtube.com/watch?v=YCtYecGbQz8",
        "http://www.youtube.com/watch?v=TBfHD2n13OA",
        "https://www.youtube.com/watch?v=UdfzC5NNAew",
        "https://www.youtube.com/watch?v=JwdGHCqb03g",
        "http://www.youtube.com/watch?v=f02gcRrdK2I",
        "https://www.youtube.com/watch?v=qekFzD7nUnc",
        "https://www.youtube.com/watch?v=ne0WwwG6D2k",
        "https://www.youtube.com/watch?v=aAfe5TZMHHI",
        "http://www.youtube.com/watch?v=7Awt-0zbqLU",
        "http://www.youtube.com/watch?v=-NGQ9INMnq0",
        "http://www.youtube.com/watch?v=rHOfQM4xjhk",
        "http://www.youtube.com/watch?v=30X2tYUGK_8",
        "http://youtube.com/watch?v=rnLWSC5p1XE",
        "http://www.youtube.com/watch?v=PrpZIklS2Kw",
        "http://www.youtube.com/watch?v=eaZORYaygo0",
        "https://www.youtube.com/watch?v=yEtRVM8LY6Q&list=PL92C3ADE426E90525&index=4",
        "https://www.youtube.com/watch?v=L7-G9VrJr_k",
        "https://www.youtube.com/watch?v=2UDFC5VxMJE",
        "https://www.youtube.com/watch?v=e4RO0VNjNuA",
        "https://www.youtube.com/watch?v=h6IBbViUzVU",
        "http://www.youtube.com/watch?v=bfSyWRvoYfw",
        "https://www.youtube.com/watch?v=K4Tq4VE8eHQ",
        "http://www.youtube.com/watch?v=8ghoXQxdk6s",
        "https://www.youtube.com/watch?v=apfgfG-Gmp4&feature=youtu.be",
        "http://www.youtube.com/user/AllanGregg#p/search/1/Uh1WLoOzS_s",
        "https://www.youtube.com/watch?v=s4bQtM7q67k",
        "https://youtu.be/jnc1Ay6X1bg",
        "http://www.youtube.com/watch?v=6HvZfzHhW5k",
        "https://www.youtube.com/watch?v=-diLmj5wJdE",
        "http://www.youtube.com/watch?v=OcSBqkLDxmo",
        "https://www.youtube.com/watch?v=1mFZM5vs1GE",
        "http://www.youtube.com/watch?v=rc91NwYvm84",
        "http://www.youtube.com/watch?v=EQVhGyH-gCQ",
        "http://www.youtube.com/watch?v=3HP-expQG7E",
        "https://www.youtube.com/watch?v=tEkJUUT8t_A",
        "https://www.youtube.com/watch?v=hdUbIlwHRkY",
        "https://www.youtube.com/watch?v=rH8SicnqSC4&feature=youtu.be",
        "http://www.youtube.com/watch?v=7TUD4gfvtDY",
        "https://www.youtube.com/watch?v=9DvmLMUfGss",
        "https://www.youtube.com/watch?v=WE50tCvldTY"
    ]

    print("Starting downloads...")
    failed_urls = []

    for url in tqdm(urls, desc="Downloading videos"):
        success = downloader.download_video(url)
        if not success:
            print(f"Failed to download: {url}")
            failed_urls.append(url)

    if failed_urls:
        print("\nFailed downloads:")
        for url in failed_urls:
            print(f"- {url}")
    else:
        print("\nAll downloads completed successfully!")


if __name__ == "__main__":
    main()