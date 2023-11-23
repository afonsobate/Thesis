import subprocess
import os
import platform

# Define the path to your virtual environment's activate script
VENV_PATH = ".venv"

# Define the three programs to run
module0 = "module0.py"
module1 = "module1.py"
module2 = "module2.py"
module3 = "module3.py"
provenance0 = "provenance0.py"
provenance1 = "provenance1.py"
provenance2 = "provenance2.py"
provenance3 = "provenance3.py"

# Get the name of the user's preferred terminal emulator
if platform.system() == "Linux":
    terminal_emulator = "x-terminal-emulator"
elif platform.system() == "Darwin":
    terminal_emulator = "Terminal"
else:
    raise Exception("Unsupported platform")

# Define the command to activate the virtual environment
if platform.system() == "Windows":
    activate_cmd = os.path.join(VENV_PATH, "Scripts", "activate.bat")
    activate_cmd_args = "/c"
else:
    activate_cmd = os.path.join(VENV_PATH, "bin", "activate")
    activate_cmd_args = "-c"

# Launch each program in a separate terminal window with the virtual environment activated
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, module0)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, module1)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, module2)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, module3)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, provenance0)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, provenance1)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, provenance2)])
subprocess.Popen([terminal_emulator, "-e", 'bash -c "source {} && python {}"'.format(activate_cmd, provenance3)])
