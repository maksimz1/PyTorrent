# Default configuration settings for the BitTorrent client

DEFAULT_CONFIG = {
    # Connection settings
    "connection_timeout": 10,  # Seconds to wait for connection
    "max_concurrent_connections": 50,  # Maximum parallel connections
    
    # Download settings
    "block_size": 16 * 1024,  # 16 KB blocks
    
    # PEX settings
    "pex_enabled": True,
    "pex_interval": 45,  # Send PEX messages every 45 seconds
    
    # Tracker settings
    "tracker_update_interval": 300,  # 5 minutes
    
    # Logging settings
    "log_dir": "logs",
    "console_log_level": "INFO"
}

# Load and save configuration functions could be added here
def get_config():
    """Return the default configuration"""
    return DEFAULT_CONFIG