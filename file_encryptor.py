import os
import multiprocessing
import logging
import psutil
from threading import Thread
from queue import Queue
import time
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_encryptor.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class CaesarCipher:
    def __init__(self, shift=3):
        self.shift = shift
    
    def encrypt_char(self, char):
        if char.isalpha():
            shifted = ord(char) + self.shift
            if char.islower():
                if shifted > ord('z'):
                    shifted -= 26
                elif shifted < ord('a'):
                    shifted += 26
            elif char.isupper():
                if shifted > ord('Z'):
                    shifted -= 26
                elif shifted < ord('A'):
                    shifted += 26
            return chr(shifted)
        return char
    
    def decrypt_char(self, char):
        if char.isalpha():
            shifted = ord(char) - self.shift
            if char.islower():
                if shifted > ord('z'):
                    shifted -= 26
                elif shifted < ord('a'):
                    shifted += 26
            elif char.isupper():
                if shifted > ord('Z'):
                    shifted -= 26
                elif shifted < ord('A'):
                    shifted += 26
            return chr(shifted)
        return char
    
    def encrypt_text(self, text):
        return ''.join([self.encrypt_char(c) for c in text])
    
    def decrypt_text(self, text):
        return ''.join([self.decrypt_char(c) for c in text])

class ProcessWorker:
    def __init__(self, cipher, process_id):
        self.cipher = cipher
        self.process_id = process_id
        self.save_queue = Queue()
        self.log_queue = Queue()
        self.save_thread = None
        self.log_thread = None
    
    def start_background_threads(self):
        self.save_thread = Thread(target=self._save_worker, daemon=True)
        self.save_thread.start()
        
        self.log_thread = Thread(target=self._log_worker, daemon=True)
        self.log_thread.start()
    
    def _save_worker(self):
        while True:
            task = self.save_queue.get()
            if task is None:
                break
            filename, content = task
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.log_queue.put(('INFO', f"Process {self.process_id}: Saved results to {filename}"))
            except Exception as e:
                self.log_queue.put(('ERROR', f"Process {self.process_id}: Failed to save to {filename}: {str(e)}"))
    
    def _log_worker(self):
        while True:
            task = self.log_queue.get()
            if task is None:
                break
            level, message = task
            if level == 'INFO':
                logger.info(message)
            elif level == 'ERROR':
                logger.error(message)
            elif level == 'DEBUG':
                logger.debug(message)
    
    def stop_background_threads(self):
        self.save_queue.put(None)
        self.log_queue.put(None)
        if self.save_thread:
            self.save_thread.join()
        if self.log_thread:
            self.log_thread.join()

def process_file_part(args):
    part, action, shift, process_id = args
    worker = ProcessWorker(CaesarCipher(shift), process_id)
    worker.start_background_threads()
    
    start_time = time.time()
    worker.log_queue.put(('DEBUG', f"Process {process_id} started {action}ing part (length: {len(part)})"))
    
    cipher = CaesarCipher(shift)
    if action == 'encrypt':
        result = cipher.encrypt_text(part)
    else:
        result = cipher.decrypt_text(part)
    
    duration = time.time() - start_time
    worker.log_queue.put(('DEBUG', f"Process {process_id} finished {action}ing part in {duration:.2f} seconds"))
    
    temp_filename = f"temp_part_{process_id}.txt"
    worker.save_queue.put((temp_filename, result))
    
    worker.stop_background_threads()
    return result

def get_available_processes():
    cpu_percent = psutil.cpu_percent(interval=1)
    total_cores = multiprocessing.cpu_count()
    
    if cpu_percent < 25:
        available = int(total_cores * 0.75)
    elif cpu_percent < 50:
        available = int(total_cores * 0.5)
    elif cpu_percent < 75:
        available = int(total_cores * 0.25)
    else:
        available = 1
    
    return max(1, min(available, total_cores))

def split_text(text, n):
    length = len(text)
    chunk_size = length // n
    chunks = []
    
    for i in range(n):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < n - 1 else length
        chunks.append(text[start:end])
    
    return chunks

def main():
    parser = argparse.ArgumentParser(description="File encryptor/decryptor using multiprocessing")
    parser.add_argument('input_file', help="Path to the input file")
    parser.add_argument('output_file', help="Path to the output file")
    parser.add_argument('--action', choices=['encrypt', 'decrypt'], required=True, 
                       help="Action to perform: encrypt or decrypt")
    parser.add_argument('--shift', type=int, default=3, 
                       help="Shift value for Caesar cipher (default: 3)")
    parser.add_argument('--processes', type=int, 
                       help="Number of processes to use (default: auto-detect based on CPU load)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        logger.error(f"Input file {args.input_file} does not exist")
        return
    
    try:
        with open(args.input_file, 'r', encoding='utf-8') as f:
            text = f.read()
    except Exception as e:
        logger.error(f"Failed to read input file: {str(e)}")
        return
    
    if args.processes:
        num_processes = min(args.processes, multiprocessing.cpu_count())
    else:
        num_processes = get_available_processes()
    
    logger.info(f"Using {num_processes} processes for {args.action}ion")
    
    chunks = split_text(text, num_processes)
    
    process_args = [(chunk, args.action, args.shift, i) for i, chunk in enumerate(chunks)]
    
    with multiprocessing.Pool(processes=num_processes) as pool:
        processed_chunks = pool.map(process_file_part, process_args)
        processed_text = ''.join(processed_chunks)
    
    try:
        with open(args.output_file, 'w', encoding='utf-8') as f:
            f.write(processed_text)
        logger.info(f"{args.action.capitalize()}ion completed successfully. Result saved to {args.output_file}")
    except Exception as e:
        logger.error(f"Failed to save result: {str(e)}")

if __name__ == '__main__':
    main()