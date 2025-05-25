import glob
import os

import numpy as np
from skimage import filters
from skimage import io
from skimage import measure
from skimage.color import rgb2gray
from skimage.segmentation import find_boundaries


def load_images_from_directory(directory_path: str, glob_pattern: str) -> list:
    """Loads images from a specified directory matching a glob pattern."""
    image_paths = []
    # Handle multiple patterns if separated by comma
    patterns = [p.strip() for p in glob_pattern.split(",")]
    for pattern in patterns:
        image_paths.extend(glob.glob(os.path.join(directory_path, pattern)))

    loaded_images = []
    if not image_paths:
        print(
            f"Warning: No images found in '{directory_path}' matching '{glob_pattern}'"
        )
    for img_path in sorted(image_paths):  # Sort for consistent order
        try:
            loaded_images.append(io.imread(img_path))
            print(f"Loaded image: {img_path}")
        except Exception as e:
            print(f"Warning: Could not load image {img_path}: {e}")
    return loaded_images


def load_and_preprocess_image(image_item):  # Argument name 'image_item' matches mapspec
    """Converts an image to grayscale."""
    print(
        f"Processing image of shape: {image_item.shape if hasattr(image_item, 'shape') else 'N/A'}"
    )
    # Handle potential alpha channel if present (e.g. in PNGs)
    if image_item.ndim == 3 and image_item.shape[2] == 4:
        image_item = image_item[..., :3]  # Drop alpha
    return rgb2gray(image_item)


def segment_image(
    gray_image,
):  # Argument name 'gray_image' matches input from previous step
    """Segments the grayscale image."""
    return filters.sobel(gray_image)


def extract_feature(segmented_image):
    """Extracts features from the segmented image."""
    # Ensure segmented_image is boolean for find_boundaries if it's float
    if not np.issubdtype(segmented_image.dtype, np.bool_):
        threshold = (
            filters.threshold_otsu(segmented_image) if np.any(segmented_image) else 0.1
        )
        image_for_boundaries = segmented_image > threshold
    else:
        image_for_boundaries = segmented_image

    boundaries = find_boundaries(image_for_boundaries)
    labeled_image = measure.label(boundaries)
    num_regions = np.max(labeled_image) if labeled_image.size > 0 else 0
    return {"num_regions": num_regions}


def classify_object(feature) -> str:
    """Classifies the object based on extracted features."""
    return "Complex" if feature["num_regions"] > 5 else "Simple"


def aggregate_results(classification):  # Receives a list of classifications
    """Aggregates classification results."""
    if not isinstance(classification, list):
        # This can happen if the pipeline is run with a single item not in a list
        # and mapspec wasn't fully engaged to produce a list.
        # For this example, we'll assume 'classification' becomes a list due to mapspec.
        classification = [classification]

    simple_count = sum(1 for c in classification if c == "Simple")
    complex_count = len(classification) - simple_count
    return {"Simple": simple_count, "Complex": complex_count}
