"""
PII Detection and Masking Module
Detects PII using Microsoft Presidio and performs masking controlled by environment variables
"""
import os
import logging
import psutil
import math
import asyncio
from typing import List, Dict, Any, Optional, Tuple, Iterator
from dataclasses import dataclass

try:
    from presidio_analyzer import AnalyzerEngine  # type: ignore
    from presidio_anonymizer import AnonymizerEngine  # type: ignore
    from presidio_analyzer.nlp_engine import NlpEngineProvider  # type: ignore
except ImportError:
    AnalyzerEngine = None  # type: ignore
    AnonymizerEngine = None  # type: ignore
    NlpEngineProvider = None  # type: ignore

logger = logging.getLogger(__name__)

@dataclass
class PIIFinding:
    """PII Detection Result"""
    entity_type: str
    start: int
    end: int
    score: float
    text: str

class PIIDetector:
    """PII Detection and Masking Class"""
    
    def __init__(self):
        self.masking_enabled = self._is_masking_enabled()
        self.detection_enabled = True  # Detection is always enabled
        self.analyzer = None
        self.anonymizer = None
        self._model_loading = False
        self._model_loaded = False
        self._initialization_task = None
        
        # Initialize alert management system
        self._initialize_alert_system()
        
        # Initialize Presidio asynchronously
        self._start_async_initialization()
    
    def _initialize_alert_system(self):
        """Initialize alert system"""
        try:
            from .alert_manager import send_pii_detection_failure_alert, send_memory_exhaustion_alert
            self._send_pii_failure_alert = send_pii_detection_failure_alert
            self._send_memory_alert = send_memory_exhaustion_alert
            self._alert_system_available = True
            logger.debug("Alert system initialized for PII detector")
        except ImportError:
            logger.warning("Alert system not available")
            self._alert_system_available = False
            self._send_pii_failure_alert = self._dummy_alert
            self._send_memory_alert = self._dummy_alert
    
    async def _dummy_alert(self, *args, **kwargs):
        """Dummy alert function (when alert system is not available)"""
        pass

    
    def _start_async_initialization(self):
        """Start asynchronous initialization"""
        try:
            # Always try async initialization first
            try:
                loop = asyncio.get_running_loop()
                self._initialization_task = asyncio.create_task(self._initialize_presidio_async())
                return
            except RuntimeError:
                # No running loop, try to create one for initialization
                pass
            
            # If we can't use async, defer initialization until first use
            logger.info("Deferring Presidio initialization until first use")
            self._model_loaded = False
            self.detection_enabled = True  # Enable but mark as not loaded
            
        except Exception as e:
            logger.warning(f"Failed to start async initialization: {e}")
            self.detection_enabled = False
    
    def _initialize_presidio_sync(self):
        """Synchronous Presidio initialization (fallback)"""
        if AnalyzerEngine is not None and NlpEngineProvider is not None:
            try:
                logger.info("Starting synchronous Presidio initialization...")
                
                # NLP engine configuration (lightweight version)
                nlp_configuration = {
                    "nlp_engine_name": "spacy",
                    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}]
                }
                
                provider = NlpEngineProvider(nlp_configuration=nlp_configuration)
                nlp_engine = provider.create_engine()
                
                self.analyzer = AnalyzerEngine(nlp_engine=nlp_engine)
                self.anonymizer = AnonymizerEngine()
                
                self._model_loaded = True
                self.detection_enabled = True
                
                masking_status = "enabled" if self.masking_enabled else "disabled"
                logger.info(f"PII detector initialized (sync) - detection: enabled, masking: {masking_status}")
                
            except Exception as e:
                logger.warning(f"Failed to initialize PII detector (sync): {e}")
                self.detection_enabled = False
        else:
            logger.warning("Presidio not available, PII detection disabled")
            self.detection_enabled = False
    
    async def _initialize_presidio_async(self):
        """Asynchronous Presidio initialization"""
        if self._model_loading:
            return
        
        self._model_loading = True
        
        try:
            logger.info("Starting asynchronous Presidio initialization...")
            
            # Execute heavy tasks in separate thread
            import concurrent.futures
            
            def _load_presidio():
                if AnalyzerEngine is None:
                    raise ImportError("Presidio not available")
                
                # NLP engine configuration
                nlp_configuration = {
                    "nlp_engine_name": "spacy",
                    "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}]
                }
                
                provider = NlpEngineProvider(nlp_configuration=nlp_configuration)
                nlp_engine = provider.create_engine()
                
                analyzer = AnalyzerEngine(nlp_engine=nlp_engine)
                anonymizer = AnonymizerEngine()
                
                return analyzer, anonymizer
            
            # Execute with ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_load_presidio)
                
                # Wait with timeout
                analyzer, anonymizer = await asyncio.wait_for(
                    asyncio.wrap_future(future), 
                    timeout=120.0  # 2 minutes timeout
                )
            
            self.analyzer = analyzer
            self.anonymizer = anonymizer
            self._model_loaded = True
            self.detection_enabled = True
            
            masking_status = "enabled" if self.masking_enabled else "disabled"
            logger.info(f"PII detector initialized (async) - detection: enabled, masking: {masking_status}")
            
        except asyncio.TimeoutError:
            logger.error("Presidio initialization timed out (120s)")
            self.detection_enabled = False
            
            if self._alert_system_available:
                await self._send_pii_failure_alert(
                    "Presidio initialization timed out after 120 seconds", 
                    "pii_detector_init"
                )
            
        except Exception as e:
            logger.warning(f"Failed to initialize PII detector (async): {e}")
            self.detection_enabled = False
            
            if self._alert_system_available:
                await self._send_pii_failure_alert(
                    f"Presidio initialization failed: {str(e)}", 
                    "pii_detector_init"
                )
            
        finally:
            self._model_loading = False
    
    async def ensure_initialized(self):
        """Ensure initialization is complete (wait if necessary)"""
        # If an analyzer has been injected (e.g., in tests), skip init
        if self.analyzer is not None:
            return
        if self._model_loaded or not self.detection_enabled:
            return
        
        if self._initialization_task and not self._initialization_task.done():
            try:
                await asyncio.wait_for(self._initialization_task, timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("Waited too long for PII detector initialization")
    
    def is_ready(self) -> bool:
        """Check if PIIDetector is ready for use"""
        return self.detection_enabled and self._model_loaded and self.analyzer is not None

    
    def _get_memory_limit_mb(self) -> int:
        """Get memory usage limit in MB (default: 30% of system memory)"""
        try:
            system_memory_mb = psutil.virtual_memory().total // (1024 * 1024)
            # Default: 30% of system memory, minimum 200MB, maximum 2GB
            default_limit = max(200, min(2048, int(system_memory_mb * 0.3)))
            
            limit_str = os.getenv("BEDROCK_KB_MEMORY_LIMIT_MB", str(default_limit))
            return max(100, int(limit_str))  # Minimum 100MB
        except:
            return 500  # Fallback value
            
    def _get_chunk_size_chars(self, text_length: int) -> int:
        """Dynamically determine chunk size based on text length"""
        memory_limit_mb = self._get_memory_limit_mb()
        
        # Assume ~4 bytes per character (Unicode), 50% margin as buffer
        max_chars = (memory_limit_mb * 1024 * 1024) // (4 * 2)
        
        # Don't split if text is small
        if text_length <= max_chars // 4:
            return text_length
            
        # Calculate appropriate chunk size (minimum 10KB, maximum 1MB characters)
        chunk_size = min(max_chars, max(10000, text_length // 10))
        return chunk_size
        
    def _chunk_text(self, text: str, chunk_size: int, overlap: int = 100) -> Iterator[Tuple[str, int, int]]:
        """Split text into chunks with overlap"""
        if len(text) <= chunk_size:
            yield text, 0, len(text)
            return
            
        start = 0
        while start < len(text):
            end = min(start + chunk_size, len(text))
            
            # Try to split at word boundary
            if end < len(text):
                # Try to extend to next whitespace character
                word_end = text.find(' ', end)
                if word_end != -1 and word_end - start < chunk_size + 200:
                    end = word_end
                    
            chunk = text[start:end]
            yield chunk, start, end
            
            if end >= len(text):
                break
                
            start = end - overlap  # Set overlap

    def _is_masking_enabled(self) -> bool:
        """Determine masking enabled/disabled from environment variable (default: true)"""
        return os.getenv("BEDROCK_KB_MASK_PII", "true").lower() in ("true", "1", "yes", "on")

    def _get_text_variants(self, text: str) -> List[str]:
        """Generate encoding variants to prevent PII bypass"""
        import base64
        import urllib.parse
        import unicodedata
        
        variants = [text]  # Original text
        
        try:
            # Try Base64 decoding
            if len(text) % 4 == 0:  # Possible Base64
                try:
                    decoded = base64.b64decode(text, validate=True).decode('utf-8')
                    variants.append(decoded)
                except (ValueError, UnicodeDecodeError):
                    pass
        except:
            pass
            
        try:
            # URL decoding
            url_decoded = urllib.parse.unquote(text)
            if url_decoded != text:
                variants.append(url_decoded)
        except:
            pass
            
        try:
            # Unicode normalization
            normalized = unicodedata.normalize('NFKC', text)
            if normalized != text:
                variants.append(normalized)
        except:
            pass
            
        # Remove separators (t.e.s.t@example.com → test@example.com)
        cleaned = ''.join(c for c in text if c.isalnum() or c in '@._-')
        if cleaned != text and len(cleaned) > 5:
            variants.append(cleaned)
            
        return list(set(variants))  # Remove duplicates

    def _deduplicate_findings(self, findings: List[PIIFinding]) -> List[PIIFinding]:
        """Remove duplicate PII detection results"""
        if not findings:
            return findings
            
        # Group by entity type and text
        unique_findings = {}
        for finding in findings:
            key = (finding.entity_type, finding.text)
            if key not in unique_findings or finding.score > unique_findings[key].score:
                unique_findings[key] = finding
                
        return list(unique_findings.values())

    def _analyze_text_chunk(self, text_chunk: str, start_offset: int, end_offset: int) -> List[PIIFinding]:
        """Analyze text chunk and return PII detection results"""
        try:
            results = self.analyzer.analyze(
                text=text_chunk,
                language="en",
                entities=[
                    "EMAIL_ADDRESS",
                    "PHONE_NUMBER",
                    "CREDIT_CARD",
                    "US_SSN",
                    "PERSON",
                    "US_PASSPORT",
                    "IP_ADDRESS"
                ]
            )
            
            findings = []
            for result in results:
                # Adjust offset (convert position in chunk to position in original text)
                finding = PIIFinding(
                    entity_type=result.entity_type,
                    start=start_offset + result.start,
                    end=start_offset + result.end,
                    score=result.score,
                    text=text_chunk[result.start:result.end]
                )
                findings.append(finding)
                
            return findings
            
        except Exception as e:
            logger.error(f"Chunk analysis failed: {type(e).__name__}")
            return []

    async def detect_pii(self, text: str) -> List[PIIFinding]:
        """Detect PII in text (encoding-aware, memory-efficient, async support)"""
        # Confirm initialization only if analyzer not already available
        if self.analyzer is None:
            await self.ensure_initialized()
        
        if not self.analyzer:
            return []
        
        # Monitor memory usage
        initial_memory_mb = psutil.virtual_memory().used // (1024 * 1024)
        memory_limit_mb = self._get_memory_limit_mb()
        
        # Check encoding variants as well
        text_variants = self._get_text_variants(text)
        all_findings = []
        
        for variant_idx, variant in enumerate(text_variants):
            if not variant or len(variant.strip()) == 0:
                continue
                
            try:
                # Split large text into chunks
                chunk_size = self._get_chunk_size_chars(len(variant))
                
                if len(variant) <= chunk_size:
                    # Process small text as is
                    findings = self._analyze_text_chunk(variant, 0, len(variant))
                    all_findings.extend(findings)
                else:
                    # Split and process large text in chunks
                    logger.info(f"Processing large text ({len(variant)} chars) in chunks of {chunk_size}")
                    
                    for chunk, start_pos, end_pos in self._chunk_text(variant, chunk_size):
                        # Check memory usage
                        current_memory_mb = psutil.virtual_memory().used // (1024 * 1024)
                        memory_used = current_memory_mb - initial_memory_mb
                        if memory_used > memory_limit_mb:
                            warning_msg = f"Memory limit exceeded ({memory_used}MB > {memory_limit_mb}MB), stopping PII detection"
                            logger.warning(warning_msg)
                            
                            # Send memory exhaustion alert
                            if self._alert_system_available:
                                try:
                                    if asyncio.get_event_loop().is_running():
                                        asyncio.create_task(self._send_memory_alert(current_memory_mb, "pii_detector_memory"))
                                except:
                                    pass
                            
                            break
                            
                        chunk_findings = self._analyze_text_chunk(chunk, start_pos, end_pos)
                        all_findings.extend(chunk_findings)
                        
                        # Promote garbage collection
                        if len(all_findings) > 1000:  # When there are large number of findings
                            import gc
                            gc.collect()
                    
            except Exception as e:
                error_msg = f"PII detection failed for variant {variant_idx}: {type(e).__name__}: {str(e)}"
                logger.error(error_msg)
                
                # Send alert (fire-and-forget as it's asynchronous)
                if self._alert_system_available:
                    try:
                        if asyncio.get_event_loop().is_running():
                            asyncio.create_task(self._send_pii_failure_alert(error_msg, "pii_detector_detection"))
                    except:
                        pass  # Ignore alert sending failure
                
                continue
        
        # Remove duplicates (when same PII is detected in multiple variants or chunks)
        unique_findings = self._deduplicate_findings(all_findings)
        
        final_memory_mb = psutil.virtual_memory().used // (1024 * 1024)
        logger.debug(f"PII detection completed. Memory usage: {final_memory_mb - initial_memory_mb}MB, Findings: {len(unique_findings)}")
        
        return unique_findings

    def _sanitize_metadata_key(self, key: str) -> str:
        """Sanitize metadata key to prevent injection attacks"""
        import re
        
        # Remove dangerous characters
        sanitized = re.sub(r'[^\w\-_.]', '_', key)
        
        # Length limitation
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
            
        # Fallback when empty
        if not sanitized:
            sanitized = "sanitized_key"
            
        return sanitized

    async def mask_pii(self, text: str) -> Tuple[str, List[PIIFinding]]:
        """Detect PII and execute full masking (complete replacement) if masking is enabled"""
        findings = await self.detect_pii(text)
        if not findings:
            return text, findings
        
        # Execute mask processing only when masking is enabled
        if self.masking_enabled:
            masked_text = self._full_mask(text, findings)
            return masked_text, findings
        else:
            # Return detection results even when masking is disabled (for warnings)
            return text, findings
        


    def _full_mask(self, text: str, findings: List[PIIFinding]) -> str:
        """Full masking (complete replacement)"""
        masked_text = text
        
        # Process from back to front so positions don't shift
        for finding in sorted(findings, key=lambda x: x.start, reverse=True):
            if finding.entity_type == "EMAIL_ADDRESS":
                replacement = "[EMAIL_REDACTED]"
            elif finding.entity_type == "PHONE_NUMBER":
                replacement = "[PHONE_REDACTED]"
            elif finding.entity_type == "CREDIT_CARD":
                replacement = "[CREDIT_CARD_REDACTED]"
            elif finding.entity_type == "US_SSN":
                replacement = "[SSN_REDACTED]"
            elif finding.entity_type == "PERSON":
                replacement = "[NAME_REDACTED]"
            elif finding.entity_type == "US_PASSPORT":
                replacement = "[PASSPORT_REDACTED]"
            elif finding.entity_type == "IP_ADDRESS":
                replacement = "[IP_REDACTED]"
            else:
                replacement = "[PII_REDACTED]"
            
            masked_text = masked_text[:finding.start] + replacement + masked_text[finding.end:]
        
        return masked_text

    def get_pii_warning(self, findings: List[PIIFinding]) -> str:
        """Generate warning message when PII is detected"""
        if not findings:
            return ""
        
        entity_counts = {}
        for finding in findings:
            entity_counts[finding.entity_type] = entity_counts.get(finding.entity_type, 0) + 1
        
        warning_parts = []
        for entity_type, count in entity_counts.items():
            warning_parts.append(f"{entity_type}: {count}")
        
        return f"⚠️ PII detected and {'masked' if self.masking_enabled else 'logged'}: {', '.join(warning_parts)}"

    async def process_metadata_safely(self, metadata: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Process metadata safely to mask PII (including key validation)"""
        if not metadata:
            return metadata, []

        warnings = []
        processed_metadata = {}

        for key, value in metadata.items():
            # Sanitize metadata key
            safe_key = self._sanitize_metadata_key(key)
            if safe_key != key:
                warnings.append(f"Metadata key sanitized: '{key}' -> '{safe_key}'")
            
            # PII detection in key itself
            key_masked, key_findings = await self.mask_pii(safe_key)
            if key_findings:
                warning = self.get_pii_warning(key_findings)
                warnings.append(f"PII detected in metadata key: {warning}")
                safe_key = key_masked

            # Value processing
            if isinstance(value, str):
                masked_value, findings = await self.mask_pii(value)
                processed_metadata[safe_key] = masked_value

                if findings:
                    warning = self.get_pii_warning(findings)
                    warnings.append(f"Metadata field '{safe_key}': {warning}")
            else:
                # Keep non-string values as is
                processed_metadata[safe_key] = value

        return processed_metadata, warnings

    async def log_pii_detection(self, content: str, findings: List[PIIFinding], context: str = ""):
        """Record PII detection to log (with masked content)"""
        if not findings:
            return
        
        # Apply masking for logging as well
        masked_content, _ = await self.mask_pii(content)
        
        entity_summary = {}
        for finding in findings:
            entity_summary[finding.entity_type] = entity_summary.get(finding.entity_type, 0) + 1
        
        logger.warning(
            f"PII detected in {context}: {entity_summary}. "
            f"Content snippet: {masked_content[:200]}..."
        )


# Global instance
pii_detector = PIIDetector()