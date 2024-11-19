.PHONY: install test lint docker-build docker-run

install:
        pip install -r requirements.txt
        pip install -r requirements-dev.txt

test:
        pytest tests/

lint:
        flake8 src/
        mypy src/
        black src/

docker-build:
        docker-compose build

docker-run:
        docker-compose up

clean:
        find . -type d -name "__pycache__" -exec rm -r {} +
        find . -type f -name "*.pyc" -delete
        find . -type f -name "*.pyo" -delete
        find . -type f -name "*.pyd" -delete
