# session_manager.py
import asyncio
import uuid
from torrent_session import TorrentSession, SessionState

class SessionManager:
    def __init__(self):
        self.sessions = {}  # session_id -> TorrentSession
        self.event_loop = asyncio.get_event_loop()
    
    async def add_torrent(self, torrent_path, download_dir="downloads", start=True):
        """Add a new torrent to the manager"""
        session_id = str(uuid.uuid4())
        session = TorrentSession(torrent_path, download_dir)
        
        # Initialize the session
        if await session.initialize():
            self.sessions[session_id] = session
            
            # Start the download if requested
            if start:
                await session.start()
            
            return session_id
        return None
    
    async def start_session(self, session_id):
        """Start or resume a session"""
        if session_id in self.sessions:
            await self.sessions[session_id].start()
    
    async def pause_session(self, session_id):
        """Pause a session"""
        if session_id in self.sessions:
            await self.sessions[session_id].pause()
    
    async def stop_session(self, session_id):
        """Stop a session"""
        if session_id in self.sessions:
            await self.sessions[session_id].stop()
    
    async def remove_session(self, session_id, delete_files=False):
        """Remove a session from the manager"""
        if session_id in self.sessions:
            await self.sessions[session_id].stop()
            
            # Optionally delete downloaded files
            if delete_files:
                # Implement file deletion
                pass
            
            del self.sessions[session_id]
    
    def get_session(self, session_id):
        """Get a session by ID"""
        return self.sessions.get(session_id)
    
    def get_all_sessions(self):
        """Get all sessions"""
        return self.sessions
        
    def get_all_session_stats(self):
        """Get stats for all sessions"""
        return {session_id: session.stats for session_id, session in self.sessions.items()}