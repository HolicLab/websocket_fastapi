FROM python:3.12

WORKDIR /app

ARG POETRY_VERSION=2.1.1
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]