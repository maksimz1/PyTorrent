# torrent_session.py
import asyncio
import time
import os
from enum import Enum
from core.torrent import Torrent
from core.tracker import TrackerHandler
from core.piece_manager import PieceManager
from network.peer_manager import AsyncPeerManager

class SessionState:
    STOPPED = 0
    DOWNLOADING = 1
    PAUSED = 2
    COMPLETED = 3
    ERROR = 4

class TorrentSession:
    def __init__(self, torrent_path, download_dir="downloads"):
        self.torrent_path = torrent_path
        self.download_dir = download_dir
        self.torrent = None
        self.tracker = None
        self.piece_manager = None
        self.peer_manager = None
        
        # Session control
        self.state = SessionState.STOPPED
        self.should_stop = False
        
        # Session stats
        self.stats = {
            'name': os.path.basename(torrent_path),
            'progress': 0.0,
            'download_speed': 0,
            'upload_speed': 0,
            'peers': 0,
            'pieces_completed': 0,
            'total_pieces': 0,
            'last_updated': time.time()
        }
        
        # Task for running the session
        self.task = None
    
    async def initialize(self):
        """Load torrent metadata"""
        try:
            self.torrent = Torrent()
            self.torrent.load_file(self.torrent_path)
            
            self.torrent.display_info()
            
            # Update initial stats
            self.stats['name'] = self.torrent.name
            self.stats['total_pieces'] = self.torrent.total_pieces
            
            return True
        except Exception as e:
            print(f"Error initializing torrent: {e}")
            self.state = SessionState.ERROR
            return False
    
    async def start(self):
        """Start or resume the torrent download"""
        if self.state == SessionState.DOWNLOADING:
            return
        
        if not self.torrent and not await self.initialize():
            return
        
        # Create or update managers
        if not self.tracker:
            self.tracker = TrackerHandler(self.torrent)
        
        if not self.piece_manager:
            self.piece_manager = PieceManager(self.torrent, self.download_dir)
            # Pass a reference to this session
            self.piece_manager.session = self
        
        # Set state to downloading - this signals all components
        self.state = SessionState.DOWNLOADING
        self.should_stop = False
        
        # Start download in a task if needed
        if not self.task or self.task.done():
            self.task = asyncio.create_task(self._download_loop())
    
    async def pause(self):
        """Pause the torrent download"""
        if self.state == SessionState.DOWNLOADING:
            # Simply change the state - no complex loops needed
            self.state = SessionState.PAUSED
            print(f"Torrent paused: {self.torrent.name}")
    
    async def stop(self):
        """Stop the torrent download"""
        if self.state in [SessionState.DOWNLOADING, SessionState.PAUSED]:
            self.should_stop = True
            # Wait for download to stop
            if self.task and not self.task.done():
                try:
                    self.task.cancel()
                    await self.task
                except asyncio.CancelledError:
                    pass
            self.state = SessionState.STOPPED
    
    async def _download_loop(self):
        """Main download loop"""
        try:
            # Create download directory
            os.makedirs(self.download_dir, exist_ok=True)
            
            # Send tracker request
            await asyncio.to_thread(self.tracker.send_request)
            
            # Create peer manager
            self.peer_manager = AsyncPeerManager(self, self.tracker, self.piece_manager, "0.0.0.0")
            # Pass session reference
            self.peer_manager.session = self
            
            # Add peers from tracker
            await self.peer_manager.add_peers()
            
            # Main download loop
            last_stats_update = time.time()
            
            while True:
                # Check for stop requests
                if self.should_stop:
                    break
                    
                # Check for pause - sleep while paused
                if self.state == SessionState.PAUSED:
                    await asyncio.sleep(0.5)
                    continue
                
                # PEX peer sharing (continues regardless of active downloads)
                await self.peer_manager.share_peers_via_pex()
                
                # Update stats periodically
                current_time = time.time()
                if current_time - last_stats_update >= 1.0:
                    self._update_stats()
                    last_stats_update = current_time
                
                # Check if download is complete
                if self.piece_manager.is_complete():
                    print("\n\n🎉 Download completed! 🎉")
                    self.state = SessionState.COMPLETED
                    break
                
                # Sleep to avoid CPU hogging
                await asyncio.sleep(1)
            
            # Clean shutdown
            if self.peer_manager:
                # Close all peer connections
                for _, peer in list(self.peer_manager.connected_peers.items()):
                    if peer.writer:
                        try:
                            peer.writer.close()
                            await peer.writer.wait_closed()
                        except Exception as e:
                            print(f"Error closing connection: {e}")
            
            # Final tracker update
            if self.state == SessionState.COMPLETED:
                await asyncio.to_thread(lambda: self.tracker.send_request("completed"))
            else:
                await asyncio.to_thread(lambda: self.tracker.send_request("stopped"))
                
        except Exception as e:
            print(f"Error in download loop: {e}")
            self.state = SessionState.ERROR
    
    def _update_stats(self):
        """Update session statistics"""
        if not self.piece_manager or not self.peer_manager:
            return
            
        current_time = time.time()
        time_diff = current_time - self.stats['last_updated']
        
        # Calculate download speed
        new_completed = len(self.piece_manager.completed_pieces)
        pieces_diff = new_completed - self.stats['pieces_completed']
        
        if time_diff > 0 and pieces_diff > 0:
            bytes_downloaded = pieces_diff * self.torrent.piece_length
            self.stats['download_speed'] = bytes_downloaded / time_diff
        else:
            self.stats['download_speed'] = 0
        
        # Update other stats
        self.stats.update({
            'progress': self.piece_manager.get_progress(),
            'pieces_completed': new_completed,
            'peers': len(self.peer_manager.connected_peers),
            'last_updated': current_time
        })