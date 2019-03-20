from georef_ar_etl.loaders import Ogr2ogrStep
from georef_ar_etl.utils import ValidateTableSchemaStep, ValidateTableSizeStep
from . import ETLTestCase


class TestOgr2ogrStep(ETLTestCase):
    def test_ogr2ogr(self):
        """El paso debería correctamente cargar un archivo .shp a una base de
        datos utilizando la herramienta externa ogr2ogr."""
        table_name = 't1'
        step = Ogr2ogrStep(table_name=table_name, geom_type='MultiPoint',
                           encoding='utf-8', metadata=self._metadata)

        # Archivo generado con georef-ar-api
        self.copy_test_file('test_shp/localidades.shp')
        self.copy_test_file('test_shp/localidades.shx')
        self.copy_test_file('test_shp/localidades.dbf')

        table = step.run('test_shp', self._ctx)
        self.assertEqual(table.__table__.name, table_name)

        ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'dpto_nombre': 'varchar',
            'dpto_id': 'varchar',
            'prov_nombre': 'varchar',
            'prov_id': 'varchar',
            'muni_nombre': 'varchar',
            'muni_id': 'varchar',
            'categoria': 'varchar',
            'centr_lat': 'varchar',
            'centr_lon': 'varchar',
            'nombre': 'varchar',
            'id': 'varchar',
            'geom': 'geometry'
        }).run(table, self._ctx)

        ValidateTableSizeStep(size=158).run(table, self._ctx)
