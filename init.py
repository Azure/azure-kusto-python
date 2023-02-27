import sys
import subprocess
import shutil

# Check if the python version is 3.8
if sys.version_info.major == 3 and sys.version_info.minor == 8:
    print("Python version is 3.8")
else:
    print("Python version is not 3.8")
    # Exit the script with an error code
    sys.exit(1)

# Try to install poetry and the project
try:
    # Check if poetry is already installed
    if shutil.which("poetry"):
        print("Poetry is already installed")
    else:
        # Run the command to install poetry
        subprocess.run(["curl", "-sSL", "https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py", "|", "python"], check=True)
    # Run the command to install the project
    subprocess.run(["poetry", "install"], check=True)
except subprocess.CalledProcessError as e:
    # Print the error message and exit with an error code
    print(e)
    sys.exit(1)

# Try to install pre commit hooks
try:
    # Run the command to install pre commit hooks
    subprocess.run(["pre-commit", "install", "-f"], check=True)
except subprocess.CalledProcessError as e:
    # Print the error message and exit with an error code
    print(e)
    sys.exit(1)

# Print a success message
print("Script completed successfully")
