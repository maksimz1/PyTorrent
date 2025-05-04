# gui_main.py
import sys
import asyncio
import qasync
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                            QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QFileDialog,
                            QProgressBar, QLabel, QHeaderView, QMessageBox, QMenu)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, pyqtSlot
from session_manager import SessionManager
from torrent_session import SessionState

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BitTorrent Client")
        self.resize(900, 600)
        
        # Create session manager
        self.session_manager = SessionManager()
        
        # Set up UI
        self.setup_ui()
        
        # Timer for stats updates
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_stats)
        self.timer.start(1000)  # Update every second
    
    def setup_ui(self):
        # Main widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Button row
        button_layout = QHBoxLayout()
        
        self.add_button = QPushButton("Add Torrent")
        self.add_button.clicked.connect(self.add_torrent)
        button_layout.addWidget(self.add_button)
        
        layout.addLayout(button_layout)
        
        # Torrents table
        self.torrents_table = QTableWidget(0, 6)
        self.torrents_table.setHorizontalHeaderLabels(
            ["Name", "Size", "Progress", "Status", "Speed", "Peers"]
        )
        self.torrents_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.torrents_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.torrents_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.torrents_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.torrents_table.customContextMenuRequested.connect(self.show_context_menu)
        
        layout.addWidget(self.torrents_table)
        
        # Status bar
        self.statusBar().showMessage("Ready")
    
    def add_torrent(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Torrent File", "", "Torrent Files (*.torrent)"
        )
        
        if file_path:
            download_dir = QFileDialog.getExistingDirectory(
                self, "Select Download Directory"
            )
            
            if download_dir:
                # Add torrent to session manager
                asyncio.create_task(self.session_manager.add_torrent(file_path, download_dir))
                self.statusBar().showMessage(f"Added torrent: {file_path}")
    
    def show_context_menu(self, position):
        # Get selected row
        index = self.torrents_table.indexAt(position)
        if not index.isValid():
            return
            
        row = index.row()
        session_id = self.torrents_table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        
        # Create context menu
        menu = QMenu(self)
        
        # Add actions based on torrent state
        session = self.session_manager.get_session(session_id)
        if not session:
            return
            
        if session.state == SessionState.DOWNLOADING:
            pause_action = menu.addAction("Pause")
            pause_action.triggered.connect(lambda: asyncio.create_task(self.session_manager.pause_session(session_id)))
        elif session.state == SessionState.PAUSED:
            resume_action = menu.addAction("Resume")
            resume_action.triggered.connect(lambda: asyncio.create_task(self.session_manager.start_session(session_id)))
        
        stop_action = menu.addAction("Stop")
        stop_action.triggered.connect(lambda: asyncio.create_task(self.session_manager.stop_session(session_id)))
        
        menu.addSeparator()
        
        remove_action = menu.addAction("Remove")
        remove_action.triggered.connect(lambda: asyncio.create_task(self.session_manager.remove_session(session_id)))
        
        # Show menu
        menu.exec(self.torrents_table.viewport().mapToGlobal(position))
    
    def update_stats(self):
        """Update the UI with current torrent stats"""
        # Get all session stats
        all_stats = self.session_manager.get_all_session_stats()
        all_sessions = self.session_manager.get_all_sessions()
        
        # Update or add rows for each session
        for session_id, stats in all_stats.items():
            session = all_sessions[session_id]
            
            # Find existing row or create new one
            row = -1
            for i in range(self.torrents_table.rowCount()):
                if self.torrents_table.item(i, 0).data(Qt.ItemDataRole.UserRole) == session_id:
                    row = i
                    break
            
            if row == -1:
                # Add new row
                row = self.torrents_table.rowCount()
                self.torrents_table.insertRow(row)
                
                # Set session ID as user data
                name_item = QTableWidgetItem(stats['name'])
                name_item.setData(Qt.ItemDataRole.UserRole, session_id)
                self.torrents_table.setItem(row, 0, name_item)
                
                # Set size
                size_item = QTableWidgetItem(self.format_size(self.get_torrent_size(session)))
                self.torrents_table.setItem(row, 1, size_item)
            
            # Update progress
            progress = stats['progress']
            progress_item = QTableWidgetItem(f"{progress:.1f}%")
            self.torrents_table.setItem(row, 2, progress_item)
            
            # Update status
            status_text = self.get_status_text(session.state)
            status_item = QTableWidgetItem(status_text)
            self.torrents_table.setItem(row, 3, status_item)
            
            # Update speed
            speed_text = self.format_speed(stats['download_speed'])
            speed_item = QTableWidgetItem(speed_text)
            self.torrents_table.setItem(row, 4, speed_item)
            
            # Update peers
            peers_item = QTableWidgetItem(str(stats['peers']))
            self.torrents_table.setItem(row, 5, peers_item)
    
    def get_torrent_size(self, session):
        """Get the total size of a torrent"""
        if session.torrent:
            return session.torrent.file_length
        return 0
    
    def get_status_text(self, state):
        """Convert session state to human-readable text"""
        return {
            SessionState.STOPPED: "Stopped",
            SessionState.DOWNLOADING: "Downloading",
            SessionState.PAUSED: "Paused",
            SessionState.COMPLETED: "Completed",
            SessionState.ERROR: "Error"
        }.get(state, "Unknown")
    
    @staticmethod
    def format_size(size_bytes):
        """Format file size in human-readable format"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.1f} GB"
    
    @staticmethod
    def format_speed(bytes_per_sec):
        """Format speed in human-readable format"""
        if bytes_per_sec < 1024:
            return f"{bytes_per_sec:.1f} B/s"
        elif bytes_per_sec < 1024 * 1024:
            return f"{bytes_per_sec/1024:.1f} KB/s"
        else:
            return f"{bytes_per_sec/(1024*1024):.1f} MB/s"

def main():
    app = QApplication(sys.argv)
    
    # Use qasync to bridge PyQt and asyncio
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)
    
    window = MainWindow()
    window.show()
    
    with loop:
        loop.run_forever()

if __name__ == "__main__":
    main()