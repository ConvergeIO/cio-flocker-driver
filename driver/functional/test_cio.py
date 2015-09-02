from twisted.trial.unittest import SynchronousTestCase
from uuid import uuid4
from bitmath import Byte, GiB
from flocker.testtools import skip_except
from flocker.node.agents.test.test_blockdevice import (
            make_iblockdeviceapi_tests
            )

from cio import CIOBlockDeviceAPI

def GetCioApiWithCleanup(test_case):
    cluster_id = uuid4()
    cioClient = CIOBlockDeviceAPI(cluster_id)
    # TODO: add cleanup of volumes created by the test.
    test_case.addCleanup(cioClient._cleanup)
    # END TODO
    return cioClient

class CIOBlockDeviceAPIInterfaceTests(
        make_iblockdeviceapi_tests(
            blockdevice_api_factory=(
                lambda test_case: GetCioApiWithCleanup(test_case)
            ),
            minimum_allocatable_size=int(GiB(8).to_Byte().value),
            device_allocation_unit=int(GiB(8).to_Byte().value),
            unknown_blockdevice_id_factory=lambda test: unicode(uuid4())
            )
        ):
        """
        Interface adherence Tests for ``CIOBlockDeviceAPI``
        """
        class CIOBlockDeviceAPIImplementationTests(SynchronousTestCase):
            """
            Implementation specific tests for ``CIOBlockDeviceAPI``.
            """
            def test_cio_api(self):
                """
                Test CIOBlockDeviceAPI Login
                """
