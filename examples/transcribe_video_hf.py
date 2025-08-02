from run_on_k8s.main import kuberun
from pathlib import Path

def read_image_as_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()
    
@kuberun(requirements=["torch", "transformers", "pillow"], cpu=8, mem="16Gi")
def image_contents(image_bytes):
    from transformers import pipeline
    from PIL import Image
    import io
    import torch

    # Load and convert the image bytes to PIL
    image = Image.open(io.BytesIO(image_bytes))
    if image.mode != "RGB":
        image = image.convert("RGB")

    # Choose a captioning model—e.g. BLIP or ViT‑GPT2
    captioner = pipeline("image-to-text", model="Salesforce/blip-image-captioning-base")
    # Alternatively: model="nlpconnect/vit-gpt2-image-captioning"

    # Run the pipeline
    outputs = captioner(image)

    # outputs is a list of dicts like [{'generated_text': "..."}]
    caption = outputs[0]["generated_text"]

    return {"description": caption}

if __name__ == "__main__":
    image_path = Path(__file__).parent / "image.png"
    image_bytes = read_image_as_bytes(str(image_path))

    result = image_contents(image_bytes)

    print("Generated caption:", result["description"])