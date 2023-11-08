from dataclasses import dataclass, field
from functools import reduce
import json
import logging
from time import time
from typing import Any, Dict, List, Optional
from dane.config import cfg


logger = logging.getLogger("DANE")
SOFTWARE_PROVENANCE_FILE = "/software_provenance.txt"
PROVENANCE_FILE = "provenance.json"


@dataclass
class Provenance:
    activity_name: str
    activity_description: str
    start_time_unix: float
    processing_time_ms: float
    input_data: Dict[str, Any]
    output_data: Dict[str, Any]
    parameters: Optional[Dict[str, Any]] = None
    software_version: Optional[Dict[str, Any]] = None
    steps: Optional[list["Provenance"]] = field(default_factory=list["Provenance"])

    def to_json(self):
        processing_steps = self.steps if self.steps else []
        return {
            "activity_name": self.activity_name,
            "activity_description": self.activity_description,
            "processing_time_ms": self.processing_time_ms,
            "start_time_unix": self.start_time_unix,
            "parameters": self.parameters,  # .to_json
            "software_version": self.software_version,  # .to_json
            "input_data": self.input_data,  # .to_json
            "output_data": self.output_data,  # .to_json
            "steps": [step.to_json() for step in processing_steps],
        }


# Generates a the main Provenance object, which will embed/include the provided provenance_chain
def generate_full_provenance_chain(
    dane_worker_id: str,
    start_time: float,
    input_data: Dict[str, str],
    provenance_chain: List[Provenance],
    provenance_file_path: str,  # where to write provenance.json
) -> Provenance:
    provenance = Provenance(
        activity_name="VisXP prep",
        activity_description=(
            "Detect shots and keyframes, "
            "extract keyframes and corresponding audio spectograms"
        ),
        start_time_unix=start_time,
        processing_time_ms=time() - start_time,
        parameters=cfg.VISXP_PREP,
        steps=provenance_chain,
        software_version=obtain_software_versions([dane_worker_id]),
        input_data=input_data,
        output_data=reduce(
            lambda a, b: {**a, **b},
            [p.output_data for p in provenance_chain],
        ),
    )

    fdata = provenance.to_json()
    logger.info("Going to write the following to disk:")
    logger.info(fdata)
    with open(provenance_file_path, "w", encoding="utf-8") as f:
        json.dump(fdata, f, ensure_ascii=False, indent=4)
        logger.info(f"Wrote provenance info to file: {provenance_file_path}")
    return provenance


# NOTE: software_provenance.txt is created while building the container image (see Dockerfile)
def obtain_software_versions(software_names):
    if isinstance(software_names, str):  # wrap a single software name in a list
        software_names = [software_names]
    try:
        with open(SOFTWARE_PROVENANCE_FILE) as f:
            urls = (
                {}
            )  # for some reason I couldnt manage a working comprehension for the below - SV
            for line in f.readlines():
                name, url = line.split(";")
                if name.strip() in software_names:
                    urls[name.strip()] = url.strip()
            assert len(urls) == len(software_names)
            return urls
    except FileNotFoundError:
        logger.info(
            f"Could not read {software_names} version"
            f"from file {SOFTWARE_PROVENANCE_FILE}: file does not exist"
        )
    except ValueError as e:
        logger.info(
            f"Could not parse {software_names} version"
            f"from file {SOFTWARE_PROVENANCE_FILE}. {e}"
        )
    except AssertionError:
        logger.info(
            f"Could not find {software_names} version"
            f"in file {SOFTWARE_PROVENANCE_FILE}"
        )
