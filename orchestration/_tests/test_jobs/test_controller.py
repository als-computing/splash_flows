"""Tests for orchestration/jobs/controller.py."""

from orchestration.jobs.controller import JobController, JobTarget


# ── Structural identity check ─────────────────────────────────────────────────

def test_hpc_alias_is_canonical_job_target():
    """HPC in bl832/job_controller.py must be the same object as JobTarget.

    Uses `is`, not `==` — equality passes even for two separate enum classes
    whose members have matching names and values. `is` catches a re-introduced
    duplicate enum class immediately.
    """
    from orchestration.flows.bl832.job_controller import HPC
    assert HPC is JobTarget


# ── JobTarget enum ────────────────────────────────────────────────────────────

class TestJobTarget:
    def test_has_alcf_member(self):
        assert JobTarget.ALCF is not None

    def test_has_nersc_member(self):
        assert JobTarget.NERSC is not None

    def test_has_olcf_member(self):
        assert JobTarget.OLCF is not None

    def test_nersc_string_value(self):
        assert JobTarget.NERSC.value == "NERSC"

    def test_alcf_string_value(self):
        assert JobTarget.ALCF.value == "ALCF"

    def test_olcf_string_value(self):
        assert JobTarget.OLCF.value == "OLCF"

    def test_membership_by_value(self):
        assert JobTarget("NERSC") is JobTarget.NERSC


# ── JobController ABC ─────────────────────────────────────────────────────────

class TestJobControllerABC:
    """JobController is an ABC but declares NO @abstractmethod.

    Direct instantiation succeeds when a valid config is provided. Tests here
    verify the contract (stores config, ABC inheritance) rather than checking
    for TypeError on instantiation.
    """

    def test_instantiates_with_valid_config(self, mock_config):
        controller = JobController(mock_config)
        assert controller is not None

    def test_stores_config(self, mock_config):
        controller = JobController(mock_config)
        assert controller.config is mock_config

    def test_subclass_inherits_config(self, mock_config):
        class MockJobController(JobController):
            pass

        controller = MockJobController(mock_config)
        assert controller.config is mock_config

    def test_is_abc_subclass(self):
        from abc import ABC
        assert issubclass(JobController, ABC)
