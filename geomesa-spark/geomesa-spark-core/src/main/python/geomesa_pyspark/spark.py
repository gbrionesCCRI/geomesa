from py4j.java_gateway import java_import
from pyspark import RDD

from geomesa_pyspark import transforms


class GeoMesaSpark:
    def __init__(self, sc):
        self.sc = sc
        self.jvm = sc._gateway.jvm

        java_import(self.jvm, "org.apache.hadoop.conf.Configuration")
        java_import(self.jvm, "org.geotools.data.Query")
        java_import(self.jvm, "org.geotools.filter.text.ecql.ECQL")

        java_import(self.jvm, "org.locationtech.geomesa.spark.api.java.JavaGeoMesaSpark")
        java_import(self.jvm, "org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider")

    def apply(self, params):
        provider = self.jvm.JavaGeoMesaSpark.apply(params)
        return SpatialRDDProvider(self.sc, params, provider)


class SpatialRDDProvider:
    def __init__(self, sc, params, provider):
        self.sc = sc
        self.jvm = sc._gateway.jvm
        self.params = params
        self.provider = provider

    def rdd_geojson(self, typename, ecql):
        return self.rdd(typename, ecql, transforms.geojson(self.jvm))

    def rdd_dict(self, typename, ecql):
        return self.rdd(typename, ecql, transforms.dict(self.jvm))

    def rdd_values(self, typename, ecql):
        return self.rdd(typename, ecql, transforms.values(self.jvm))

    def rdd_tuples(self, typename, ecql):
        return self.rdd(typename, ecql, transforms.tuples(self.jvm))

    def rdd(self, typename, ecql, transform):
        filter = self.jvm.ECQL.toFilter(ecql)
        query = self.jvm.Query(typename, filter)
        jrdd = self.provider.rdd(self.jvm.Configuration(), self.sc._jsc, self.params, query, transform)
        return RDD(self.jvm.SerDe.javaToPython(jrdd), self.sc)
