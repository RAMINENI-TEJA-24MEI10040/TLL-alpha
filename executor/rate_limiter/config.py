import yaml
import logging
import os
from pydantic import BaseModel
from typing import Dict

logger = logging.getLogger(__name__)

class RateLimitConfig(BaseModel):
    global_limit: float = 1000.0  # requests per second
    domains: Dict[str, float] = {}

class RateLimiterConfigLoader:
    def __init__(self, config_path: str = "rate_limit.yaml"):
        self.config_path = config_path
        self._config = RateLimitConfig()
        self.load()

    def load(self):
        """Loads or reloads configuration from the YAML file."""
        if not os.path.exists(self.config_path):
            logger.warning(f"Rate limit config {self.config_path} not found. Using defaults.")
            self._config = RateLimitConfig()
            return
            
        try:
            with open(self.config_path, "r") as f:
                data = yaml.safe_load(f)
                if data:
                    self._config = RateLimitConfig(**data)
            logger.info(f"Loaded rate limits: Global={self._config.global_limit}, Domains={len(self._config.domains)}")
        except Exception as e:
            logger.error(f"Failed to load {self.config_path}: {e}")

    @property
    def config(self) -> RateLimitConfig:
        return self._config
