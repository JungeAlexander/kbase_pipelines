# kbase pipelines

## Installation

```
curl -sSL https://install.astronomer.io | sudo bash -s -- v0.23.2
```

```
poetry install --no-root
poetry run pre-commit install
```

## Running

```
astro dev start
```

If this should not work, try:

```
DOCKER_BUILDKIT=0 astro dev start
```


```
astro dev stop
```

## Debugging

Use VS Code to connect to the Docker container running Airflow's scheduler.

Open directory in VS Code: `/usr/local/airflow/`

Install VS Code Python (and Pylance) extension if needed and set interpreter to: `/usr/local/bin/python`.

Use following `launch.json`:

```{json}
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Airflow test",
            "type": "python",
            "request": "launch",
            "program": "/usr/local/bin/airflow",
            "console": "integratedTerminal",
            // "args": [
            //     "dags",
            //     "test",
            //     "python_bytes_dag",
            //     "2021-02-11"
            // ]
            "args": [
                "tasks",
                "test",
                "python_bytes_dag",
                "get_new_episodes",
                "2021-02-11"
            ]
        }
    ]
}
```
