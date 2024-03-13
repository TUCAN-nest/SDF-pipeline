# Architecture

![pipeline architecture](architecture.svg)

## Consumer API

A consumer is a Python function that takes the following arguments:

* `molfile`: `str` containing molblock
* `get_molfile_id`: `Callable` (aka function) that parses an (arbitrary) identifier from `molfile`

The consumer function has to return a `ConsumerResult`,
which is a [Pydantic](https://docs.pydantic.dev/latest/) model whose schema you can inspect with

```Python
import json
from sdf_pipeline.drivers import ConsumerResult

print(json.dumps(ConsumerResult.model_json_schema(), indent=2))
```

Below you find a minimal example of how to write a consumer function.

```Python
import textwrap
from typing import Callable
from sdf_pipeline.drivers import ConsumerResult


molfile: str = textwrap.dedent(
    """
    https://en.wikipedia.org/wiki/This_Is_Water
      -OEChem-03132411562D

      3  2  0     0  0  0  0  0  0999 V2000
        2.5369   -0.1550    0.0000 O   0  0  0  0  0  0  0  0  0  0  0  0
        3.0739    0.1550    0.0000 H   0  0  0  0  0  0  0  0  0  0  0  0
        2.0000    0.1550    0.0000 H   0  0  0  0  0  0  0  0  0  0  0  0
      1  2  1  0  0  0  0
      1  3  1  0  0  0  0
    M  END
    """
)


def get_molfile_id(molfile: str) -> str:
    return molfile.splitlines()[0].strip()


def example_consumer(molfile: str, get_molfile_id: Callable) -> ConsumerResult:
    return ConsumerResult(
        molfile_id=get_molfile_id(molfile),
        info={"consumer": "example"},
        result={"molfile_length": len(molfile)},
    )


print(example_consumer(molfile, get_molfile_id))
```

## Drivers

The [tests](tests/test_drivers.py) show how to pass a consumer function to the pipeline via the drivers.
Run the tests with

```Shell
pytest
```
