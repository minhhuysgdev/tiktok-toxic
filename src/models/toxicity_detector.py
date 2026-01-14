"""
Toxicity Detection Model - Wrapper cho ViHateT5 models
"""
import logging
import warnings
from typing import List, Union

import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

# Suppress HuggingFace deprecation warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="huggingface_hub")

logger = logging.getLogger(__name__)


class ToxicityDetector:
    """
    Model để phát hiện hate speech/toxicity trong văn bản tiếng Việt
    Sử dụng các model từ series ViHateT5 của tarudesu
    """
    
    # Mapping labels
    LABEL_MAPPING = {
        "HATE": "HATE",
        "OFFENSIVE": "OFFENSIVE", 
        "CLEAN": "CLEAN"
    }
    
    def __init__(
        self, 
        model_name: str = "tarudesu/ViHateT5-HSD",
        device: str = "cpu",
        max_length: int = 256,
        batch_size: int = 32
    ):
        """
        Khởi tạo model
        
        Args:
            model_name: Tên model trên HuggingFace
            device: Device để chạy model (cpu/cuda)
            max_length: Độ dài tối đa của input
            batch_size: Batch size khi predict nhiều text
        """
        self.model_name = model_name
        self.device = device
        self.max_length = max_length
        self.batch_size = batch_size

        # Detect if this is base model or fine-tuned model
        self.is_base_model = "base" in model_name and "HSD" not in model_name
        # Simple zero-shot prompting for base model
        self.task_prefix = "classify this text as CLEAN, OFFENSIVE, or HATE: " if self.is_base_model else ""
        
        logger.info(f"Loading model: {model_name}")
        
        try:
            # Load tokenizer và model
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
            
            # Move model to device
            if device == "cuda" and torch.cuda.is_available():
                self.model = self.model.cuda()
                logger.info("✓ Model loaded on GPU")
            else:
                self.model = self.model.cpu()
                logger.info("✓ Model loaded on CPU")
            
            self.model.eval()  # Set to evaluation mode
            
            logger.info(f"✓ ToxicityDetector initialized successfully")
        
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def _decode_prediction(self, generated_ids) -> str:
        """
        Decode prediction từ model output
        
        Args:
            generated_ids: Output IDs từ model
            
        Returns:
            Label: HATE, OFFENSIVE, hoặc CLEAN
        """
        decoded = self.tokenizer.decode(generated_ids[0], skip_special_tokens=True)
        label = decoded.strip().upper()
        
        # Normalize label
        if "HATE" in label:
            return "HATE"
        elif "OFFENSIVE" in label or "OFFENS" in label:
            return "OFFENSIVE"
        else:
            return "CLEAN"
    
    def predict(self, text: str) -> str:
        """
        Dự đoán toxicity cho một văn bản
        
        Args:
            text: Văn bản cần phân tích
            
        Returns:
            Label: HATE, OFFENSIVE, hoặc CLEAN
        """
        if not text or not text.strip():
            return "CLEAN"
        
        try:
            # Add task prefix for base model
            input_text = self.task_prefix + text

            # Tokenize
            inputs = self.tokenizer(
                input_text,
                max_length=self.max_length,
                padding="max_length",
                truncation=True,
                return_tensors="pt"
            )
            
            # Move to device
            if self.device == "cuda":
                inputs = {k: v.cuda() for k, v in inputs.items()}
            
            # Generate prediction
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_length=10,
                    num_beams=1
                )
            
            # Decode
            label = self._decode_prediction(outputs)
            return label
        
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return "CLEAN"  # Default fallback
    
    def predict_batch(self, texts: List[str]) -> List[str]:
        """
        Dự đoán toxicity cho nhiều văn bản (batch processing)
        
        Args:
            texts: Danh sách văn bản cần phân tích
            
        Returns:
            Danh sách labels tương ứng
        """
        if not texts:
            return []
        
        results = []
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i:i + self.batch_size]
            batch_results = self._predict_batch_internal(batch_texts)
            results.extend(batch_results)
        
        return results
    
    def _predict_batch_internal(self, texts: List[str]) -> List[str]:
        """
        Xử lý một batch văn bản
        
        Args:
            texts: Batch văn bản
            
        Returns:
            Batch labels
        """
        # Replace empty texts
        texts = [t if t and t.strip() else " " for t in texts]
        
        try:
            # Tokenize batch
            inputs = self.tokenizer(
                texts,
                max_length=self.max_length,
                padding="max_length",
                truncation=True,
                return_tensors="pt"
            )
            
            # Move to device
            if self.device == "cuda":
                inputs = {k: v.cuda() for k, v in inputs.items()}
            
            # Generate predictions
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_length=10,
                    num_beams=1
                )
            
            # Decode all predictions
            labels = []
            for output in outputs:
                label = self._decode_prediction([output])
                labels.append(label)
            
            return labels
        
        except Exception as e:
            logger.error(f"Batch prediction error: {e}")
            return ["CLEAN"] * len(texts)
    
    def is_toxic(self, text: str) -> bool:
        """
        Kiểm tra xem văn bản có toxic không
        
        Args:
            text: Văn bản cần kiểm tra
            
        Returns:
            True nếu toxic (HATE hoặc OFFENSIVE)
        """
        label = self.predict(text)
        return label in ["HATE", "OFFENSIVE"]
    
    def get_toxicity_score(self, text: str) -> float:
        """
        Tính điểm toxicity (0-1)
        
        Args:
            text: Văn bản cần phân tích
            
        Returns:
            Score: 0.0 (CLEAN), 0.5 (OFFENSIVE), 1.0 (HATE)
        """
        label = self.predict(text)
        
        score_map = {
            "CLEAN": 0.0,
            "OFFENSIVE": 0.5,
            "HATE": 1.0
        }
        
        return score_map.get(label, 0.0)


def create_spark_udf(model_name: str = "tarudesu/ViHateT5-HSD", device: str = "cpu", batch_size: int = 32):
    """
    Tạo Spark Pandas UDF để xử lý theo batch (nhanh hơn nhiều so với UDF thông thường)
    
    Args:
        model_name: Tên model
        device: Device
        batch_size: Batch size cho pandas UDF
        
    Returns:
        Spark Pandas UDF function
    """
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType
    import pandas as pd
    
    # Khởi tạo model (sẽ được cache trên mỗi executor)
    detector = None
    
    @pandas_udf(StringType())
    def predict_batch_udf(texts: pd.Series) -> pd.Series:
        """
        Xử lý batch comments cùng lúc thay vì từng cái một
        """
        nonlocal detector
        if detector is None:
            logger.info(f"🔄 Initializing ToxicityDetector on executor (model: {model_name}, device: {device})")
            detector = ToxicityDetector(model_name=model_name, device=device, batch_size=batch_size)
            logger.info("✓ ToxicityDetector initialized on executor")
        
        # Convert pandas Series to list
        texts_list = texts.tolist()
        
        # Xử lý theo batch để tận dụng batch processing của model
        results = detector.predict_batch(texts_list)
        
        # Trả về pandas Series
        return pd.Series(results)
    
    return predict_batch_udf


if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    
    detector = ToxicityDetector()
    
    # Test cases
    test_texts = [
        "Xin chào, bạn khỏe không?",
        "Đồ ngu ngốc!",
        "Chúng mày là lũ khốn nạn",
    ]
    
    print("\n=== Testing Toxicity Detector ===")
    for text in test_texts:
        label = detector.predict(text)
        score = detector.get_toxicity_score(text)
        print(f"Text: {text}")
        print(f"Label: {label}, Score: {score}\n")
    
    # Test batch
    print("\n=== Testing Batch Prediction ===")
    labels = detector.predict_batch(test_texts)
    for text, label in zip(test_texts, labels):
        print(f"{text} -> {label}")

