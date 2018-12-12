# lta
Long Term Archive

## Development

### Installing Python 3.7 on Linux Mint
Source: https://tecadmin.net/install-python-3-7-on-ubuntu-linuxmint/

    sudo apt-get install build-essential
    sudo apt-get install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev
    wget https://www.python.org/ftp/python/3.7.1/Python-3.7.1.tar.xz
    tar xvJf Python-3.7.1.tar.xz
    cd Python-3.7.1/
    ./configure --enable-optimizations
    sudo make altinstall

### Establishing a development environment
Follow these steps to get a development environment for LTA:

    cd ~/projects
    git clone git@github.com:WIPACrepo/lta.git
    cd lta
    python3.7 -m venv ./env
    source env/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt

### Updating requirements.txt after a pip install
If you install a new package using `pip install cool-pkg-here` then
be sure to update the `requirements.txt` file with the following
command:

    pip freeze --all >requirements.txt

### Helper script
There is a helper script `snake` that defines some common project
tasks.

    Try one of the following tasks:

    snake clean                # Remove build cruft
    snake coverage             # Perform coverage analysis
    snake dist                 # Create a distribution tarball and wheel
    snake lint                 # Run static analysis tools
    snake rebuild              # Rebuild the module

The task `rebuild` doesn't really build (no need to compile Python),
but it does run the unit tests.

### Bumping to the next version
If you need to increase the version number of the project, don't
forget to edit the following:

    CHANGELOG.md
    lta/__init__.py
