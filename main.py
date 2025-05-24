# main.py
import sys
import asyncio
import qasync
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                            QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QFileDialog,
                            QProgressBar, QLabel, QHeaderView, QMessageBox, QMenu)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, pyqtSlot, QSize
from PyQt6.QtGui import QPainter, QColor, QPen, QLinearGradient
from session_manager import SessionManager
from torrent_session import SessionState


class PieceProgressBar(QWidget):
    """
    Custom widget to display BitTorrent piece download status as a series of lines
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Piece data
        self.completed_pieces = set()
        self.total_pieces = 0
        self.busy_pieces = set()  # Pieces currently being downloaded
        
        # Visual settings
        self.min_height = 24
        self.downloaded_color = QColor(46, 204, 113)  # Green for completed pieces
        self.pending_color = QColor(189, 195, 199)    # Light gray for pending pieces
        self.busy_color = QColor(52, 152, 219)        # Blue for busy pieces
        self.spacing = 1  # Spacing between lines
        
        # Set minimum size
        self.setMinimumHeight(self.min_height)
        
    def sizeHint(self):
        return QSize(200, self.min_height)
        
    def setData(self, completed_pieces, total_pieces, busy_pieces=None):
        """Update the widget with new piece data"""
        self.completed_pieces = completed_pieces
        self.total_pieces = total_pieces
        self.busy_pieces = busy_pieces or set()
        
        # Update tooltip with stats
        percent_complete = (len(self.completed_pieces) / self.total_pieces * 100) if self.total_pieces > 0 else 0
        tooltip = (f"Pieces: {len(self.completed_pieces)}/{self.total_pieces} ({percent_complete:.1f}%)\n"
                  f"Active downloads: {len(self.busy_pieces)}")
        self.setToolTip(tooltip)
        
        self.update()  # Request a repaint
    
    def paintEvent(self, event):
        """Draw the piece status bars"""
        if self.total_pieces <= 0:
            return
            
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        width = self.width()
        height = self.height()
        
        # Create a rounded rectangle background with a gradient
        gradient = QLinearGradient(0, 0, 0, height)
        gradient.setColorAt(0, QColor(240, 240, 240))
        gradient.setColorAt(1, QColor(220, 220, 220))
        
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(gradient)
        painter.drawRoundedRect(0, 0, width, height, 5, 5)
        
        # Calculate bar width (ensure at least 1 pixel per piece)
        bar_width = max(1, (width - (self.total_pieces-1) * self.spacing) / self.total_pieces)
        
        # May need to adjust spacing if we have many pieces
        actual_spacing = self.spacing
        if bar_width <= 1:
            bar_width = 1
            actual_spacing = 0
        
        # Draw each piece
        for i in range(self.total_pieces):
            # Set color based on download status
            if i in self.completed_pieces:
                painter.setPen(QPen(self.downloaded_color, bar_width))
            elif i in self.busy_pieces:
                painter.setPen(QPen(self.busy_color, bar_width))
            else:
                painter.setPen(QPen(self.pending_color, bar_width))
            
            # Calculate x position
            x = i * (bar_width + actual_spacing) + bar_width/2
            
            # Draw a vertical line for each piece
            painter.drawLine(int(x), 4, int(x), height-4)
            
        # Draw a border around the widget
        painter.setPen(QPen(QColor(180, 180, 180), 1))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawRoundedRect(0, 0, width-1, height-1, 5, 5)


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
        self.torrents_table = QTableWidget(0, 7)  # Added a column for the visual progress bar
        self.torrents_table.setHorizontalHeaderLabels(
            ["Name", "Size", "Progress", "Visual Progress", "Status", "Speed", "Peers"]
        )
        self.torrents_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.torrents_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)  # Make visual progress stretch
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
        
        # Clear existing(expired) rows
        for row in range(self.torrents_table.rowCount() - 1, -1, -1):
            session_id = self.torrents_table.item(row, 0).data(Qt.ItemDataRole.UserRole)
            if session_id not in (all_sessions.keys()):
                print(f"Removing row {row} for deleted session {session_id}")
                self.torrents_table.removeRow(row)
                
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
                
                # Create visual progress bar widget
                progress_widget = PieceProgressBar()
                self.torrents_table.setCellWidget(row, 3, progress_widget)
            
            # Update progress percentage
            progress = stats['progress']
            progress_item = QTableWidgetItem(f"{progress:.1f}%")
            self.torrents_table.setItem(row, 2, progress_item)
            
            # Update visual progress bar
            progress_widget = self.torrents_table.cellWidget(row, 3)
            if progress_widget and isinstance(progress_widget, PieceProgressBar):
                if session.piece_manager:
                    completed_pieces = session.piece_manager.completed_pieces
                    busy_pieces = session.piece_manager.busy_pieces
                    total_pieces = session.torrent.total_pieces
                    progress_widget.setData(completed_pieces, total_pieces, busy_pieces)
            
            # Update status
            status_text = self.get_status_text(session.state)
            status_item = QTableWidgetItem(status_text)
            self.torrents_table.setItem(row, 4, status_item)
            
            # Update speed
            speed_text = self.format_speed(stats['download_speed'])
            speed_item = QTableWidgetItem(speed_text)
            self.torrents_table.setItem(row, 5, speed_item)
            
            # Update peers
            peers_item = QTableWidgetItem(str(stats['peers']))
            self.torrents_table.setItem(row, 6, peers_item)
    
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