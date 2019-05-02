from georef_ar_etl.models import Province
from georef_ar_etl.exceptions import ProcessException
from georef_ar_etl.provinces import ProvincesExtractionStep
from . import ETLTestCase


class TestEntitiesExtractionStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self._tmp_provinces = self.create_test_provinces()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Province).delete()
        self._ctx.session.query(self._tmp_provinces).delete()
        super().tearDown()

    def test_repeated_tmp_entity(self):
        """Si una tabla temporal (tmp_) contiene dos entidade con el mismo
        código, debería lanzarse una excepción durante el proceso de extracción
        para cualquier clase que herede de EntitiesExtractionStep."""
        step = ProvincesExtractionStep()

        prov = self._ctx.session.query(self._tmp_provinces).first()
        self._ctx.session.add(self._tmp_provinces(in1=prov.in1))
        self._ctx.session.commit()

        with self.assertRaisesRegex(ProcessException, 'Clave primaria'):
            step.run(self._tmp_provinces, self._ctx)
