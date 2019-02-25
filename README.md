# eric-tenantbase

If you do not have virtualenv, install it.

    pip3 install virtualenv

Create a virtual environment

    virtualenv -p `which python3` env

Use virtual environment

    source env/bin/activate

Install requirements

    pip3 install -r requirements.txt

Execute server

    python3 main.py serve database.sqlite

