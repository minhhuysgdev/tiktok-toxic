# scripts/preload_model.py
"""
Script để preload và cache model trước khi chạy Spark streaming
"""
import logging
import sys
import warnings
from pathlib import Path

# Suppress HuggingFace deprecation warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="huggingface_hub")

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.toxicity_detector import ToxicityDetector
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Preload model để cache"""
    # Load config
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    model_config = config['model']
    
    logger.info("=" * 60)
    logger.info("Preloading Toxicity Detection Model")
    logger.info("=" * 60)
    logger.info(f"Model: {model_config['name']}")
    logger.info(f"Device: {model_config['device']}")
    logger.info("")
    logger.info("This will download and cache the model...")
    logger.info("")
    
    try:
        logger.info("Starting model download...")
        logger.info("This may take a few minutes on first run...")
        
        # Load model (sẽ tự động cache vào ~/.cache/huggingface/)
        detector = ToxicityDetector(
            model_name=model_config['name'],
            device=model_config['device'],
            max_length=model_config.get('max_length', 256),
            batch_size=model_config.get('batch_size', 32)
        )
        
        logger.info("✓ Model loaded successfully!")
        
        # Test với một text để đảm bảo model hoạt động
        test_text = "Xin chào"
        result = detector.predict(test_text)
        logger.info(f"✓ Test prediction: '{test_text}' -> {result}")
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("✓ Model successfully cached!")
        logger.info("=" * 60)
        
        # Check actual cache location
        import os
        cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
        model_cache_name = f"models--{model_config['name'].replace('/', '--')}"
        cache_path = os.path.join(cache_dir, model_cache_name)
        
        if os.path.exists(cache_path):
            logger.info(f"✓ Cache location exists: {cache_path}")
            # List files in cache
            import subprocess
            result = subprocess.run(['ls', '-lh', cache_path], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"Cache contents:\n{result.stdout}")
        else:
            logger.warning(f"Cache location not found: {cache_path}")
            logger.info(f"Cache directory exists: {os.path.exists(cache_dir)}")
            if os.path.exists(cache_dir):
                logger.info(f"Available models in cache:")
                result = subprocess.run(['ls', '-d', os.path.join(cache_dir, 'models--*')], capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info(result.stdout)
        
    except Exception as e:
        logger.error(f"Failed to preload model: {e}", exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()