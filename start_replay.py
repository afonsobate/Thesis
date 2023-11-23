import subprocess
import os
import platform

# Define the path to your virtual environment's activate script
VENV_PATH = ".venv"

# Define the three programs to run
module0 = "replay0.py"
module1 = "replay1.py"
module2 = "replay2.py"
module3 = "replay3.py"

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
