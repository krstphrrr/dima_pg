from PyQt5.QtWidgets import *


app = QApplication([])
button = QPushButton('Click')
app.setStyle('Windows')
app.setStyleSheet("QPushButton { margin: 10ex; }")


def on_button_clicked():
    alert = QMessageBox()
    alert.setText('You clicked the button!')
    alert.exec_()

button.clicked.connect(on_button_clicked)
button.show()
app.exec_()
