# gui/main_window.py

from PySide6.QtWidgets import QLabel
from core.perm_manager import PermManager
from pathlib import Path

from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QPushButton, QHBoxLayout, QStackedWidget
from gui.views.ranking_view import RankingView
from gui.views.registry_view import RegistryView
from gui.views.candle_download_view import CandleDownloadView
from gui.views.augmenter_view import AugmenterView  # 🆕 NEW
from gui.views.parquet_view import ParquetViewer  # ✅ NEW VIEW
from gui.views.backtest_MA5_view import BacktestMA5View  # ✅ NEW


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("순매수 분석 도구 (PySide6)")

        # Status bar
        self.status_label = QLabel("📡 Perm data status: checking...")
        self.statusBar().addWidget(self.status_label)

        # Set base directory
        self.base_dir = Path.cwd()

        # Trigger perm updates
        self.perm_manager = PermManager(
            base_dir=self.base_dir,
            status_callback=self.update_status
        )
        self.perm_manager.check_updates()

        self.setGeometry(100, 100, 1200, 800)

        # Views
        self.stack = QStackedWidget()
        self.ranking_view = RankingView()
        self.registry_view = RegistryView(base_dir=self.base_dir)
        self.candle_view = CandleDownloadView()  # 🆕 NEW VIEW
        self.augmenter_view = AugmenterView()  # 🆕 NEW VIEW
        self.parquet_viewer = ParquetViewer()  # ✅ NEW VIEW
        self.backtest_view = BacktestMA5View()

        # Add views to stack
        self.stack.addWidget(self.ranking_view)
        self.stack.addWidget(self.registry_view)
        self.stack.addWidget(self.candle_view)  # 🆕
        self.stack.addWidget(self.augmenter_view)  # 🆕
        self.stack.addWidget(self.parquet_viewer)  # ✅ NEW VIEW
        self.stack.addWidget(self.backtest_view)

        self.init_ui()

    def init_ui(self):
        # Side navigation
        nav_widget = QWidget()
        nav_layout = QVBoxLayout(nav_widget)

        btn_ranking = QPushButton("📊 순매수 통합 보기")
        btn_registry = QPushButton("📁 파일 레지스트리")
        btn_candle = QPushButton("🕯️ 일봉차트 다운로드")  # 🆕
        btn_augmenter = QPushButton("📦 데이터 증강기")  # 🆕
        btn_parquet = QPushButton("📂 Combined Parquet 뷰어")  # ✅ NEW
        btn_backtest = QPushButton("🧪 전략 테스트")  # ✅ NEW

        btn_ranking.clicked.connect(lambda: self.stack.setCurrentWidget(self.ranking_view))
        btn_registry.clicked.connect(lambda: self.stack.setCurrentWidget(self.registry_view))
        btn_candle.clicked.connect(lambda: self.stack.setCurrentWidget(self.candle_view))  # 🆕
        btn_augmenter.clicked.connect(lambda: self.stack.setCurrentWidget(self.augmenter_view))  # 🆕
        btn_parquet.clicked.connect(lambda: self.stack.setCurrentWidget(self.parquet_viewer))  # ✅ NEW
        btn_backtest.clicked.connect(lambda: self.stack.setCurrentWidget(self.backtest_view))  # ✅

        nav_layout.addWidget(btn_ranking)
        nav_layout.addWidget(btn_registry)
        nav_layout.addWidget(btn_candle)  # 🆕
        nav_layout.addWidget(btn_augmenter)  # 🆕
        nav_layout.addWidget(btn_parquet)  # ✅ NEW
        nav_layout.addWidget(btn_backtest)  # ✅
        nav_layout.addStretch()

        # Main layout
        main_widget = QWidget()
        main_layout = QHBoxLayout(main_widget)
        main_layout.addWidget(nav_widget)
        main_layout.addWidget(self.stack, 1)

        self.setCentralWidget(main_widget)

    def update_status(self, msg: str):
        self.status_label.setText(f"📡 {msg}")
