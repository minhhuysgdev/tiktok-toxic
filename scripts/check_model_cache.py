#!/usr/bin/env python3
"""
Script để kiểm tra xem model đã được cache chưa
"""
import os
from pathlib import Path

def check_model_cache():
    """Kiểm tra cache của model"""
    model_name = "tarudesu/ViHateT5-base-HSD"
    cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
    model_cache_name = f"models--{model_name.replace('/', '--')}"
    cache_path = os.path.join(cache_dir, model_cache_name)
    
    print("=" * 60)
    print("Checking Model Cache")
    print("=" * 60)
    print(f"Model: {model_name}")
    print(f"Cache path: {cache_path}")
    print()
    
    if os.path.exists(cache_path):
        # Get size
        import subprocess
        result = subprocess.run(['du', '-sh', cache_path], capture_output=True, text=True)
        size = result.stdout.split()[0] if result.returncode == 0 else "unknown"
        
        print(f"✓ Model đã được cache!")
        print(f"  Size: {size}")
        print(f"  Path: {cache_path}")
        
        # Check for model files
        snapshots_dir = os.path.join(cache_path, "snapshots")
        if os.path.exists(snapshots_dir):
            snapshots = [d for d in os.listdir(snapshots_dir) if os.path.isdir(os.path.join(snapshots_dir, d))]
            if snapshots:
                print(f"  Snapshots: {len(snapshots)} snapshot(s)")
                latest_snapshot = os.path.join(snapshots_dir, snapshots[0])
                model_files = []
                for root, dirs, files in os.walk(latest_snapshot):
                    for file in files:
                        if file.endswith(('.bin', '.safetensors', '.json')):
                            model_files.append(file)
                if model_files:
                    print(f"  Model files found: {len(model_files)} file(s)")
                    print(f"    Examples: {', '.join(model_files[:3])}")
        
        return True
    else:
        print("✗ Model chưa được cache!")
        print(f"  Cache directory không tồn tại: {cache_path}")
        
        # Check if cache dir exists
        if os.path.exists(cache_dir):
            print(f"  Cache directory tồn tại: {cache_dir}")
            # List available models
            try:
                result = subprocess.run(['ls', '-d', os.path.join(cache_dir, 'models--*')], 
                                      capture_output=True, text=True)
                if result.returncode == 0 and result.stdout.strip():
                    print(f"  Available models in cache:")
                    for line in result.stdout.strip().split('\n'):
                        model_name_only = os.path.basename(line)
                        print(f"    - {model_name_only}")
            except:
                pass
        else:
            print(f"  Cache directory không tồn tại: {cache_dir}")
        
        return False

if __name__ == "__main__":
    import subprocess
    cached = check_model_cache()
    print()
    print("=" * 60)
    if cached:
        print("✓ Model đã sẵn sàng để sử dụng!")
    else:
        print("✗ Cần chạy: python scripts/preload_model.py")
    print("=" * 60)
