from twisted.trial.unittest import SynchronousTestCase
from uuid import uuid4
from bitmath import Byte, GiB
from flocker.testtools import skip_except
from flocker.node.agents.test.test_blockdevice import (
            make_iblockdeviceapi_tests,make_iprofiledblockdeviceapi_tests
            )

from flocker.node.agents.blockdevice import MandatoryProfiles
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

        def test_create_volume_gold_profile(self):
           """
           Requesting ``gold`` profile during volume creation honors
           ``gold`` attributes.
           """
           self._assert_create_volume_with_mandatory_profile(
              MandatoryProfiles.GOLD)

        def test_create_too_large_volume_with_profile(self):
          """
          Create a volume so large that none of the ``MandatoryProfiles``
          can be assigned to it.
          """
          self.assertRaises(Exception,
                            self._assert_create_volume_with_mandatory_profile,
                            MandatoryProfiles.GOLD,
                            size_GiB=1024*1024)

        def test_create_volume_silver_profile(self):
           """
           Requesting ``silver`` profile during volume creation honors
           ``silver`` attributes.
           """
           self._assert_create_volume_with_mandatory_profile(
            MandatoryProfiles.SILVER)
        
        def test_create_too_large_volume_silver_profile(self):
           """
           Too large volume (> 64TiB) for ``silver`` profile.
           """
           self.assertRaises(Exception,
                          self._assert_create_volume_with_mandatory_profile,
                          MandatoryProfiles.SILVER,
                          size_GiB=1024*1024)

        def test_create_volume_bronze_profile(self):
           """
           Requesting ``bronze`` profile during volume creation honors
           ``bronze`` attributes.
           """
           self._assert_create_volume_with_mandatory_profile(
              MandatoryProfiles.BRONZE)

        def _assert_create_volume_with_mandatory_profile(self, profile,
                                                     created_profile=None,
                                                     size_GiB=4):
           """
           Volume created with given profile has the attributes
           expected from the profile.

           :param ValueConstant profile: Name of profile to use for creation.
           :param ValueConstant created_profile: Name of the profile volume is
            expected to be created with.
           :param int size_GiB: Size of volume to be created.
           """
           if created_profile is None:
              created_profile = profile
           volume1 = self.api.create_volume_with_profile(
              dataset_id=uuid4(),
              size=self.minimum_allocatable_size * size_GiB,
              profile_name=profile.value)

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
