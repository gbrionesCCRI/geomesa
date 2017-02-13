def geojson(jvm):
    return jvm.JavaSpatialRDDProvider.GeoJSONStringTransform

def dict(jvm):
    return jvm.JavaSpatialRDDProvider.PyMapTransform

def values(jvm):
    return jvm.JavaSpatialRDDProvider.PyValueListTransform

def tuples(jvm):
    return jvm.JavaSpatialRDDProvider.PyKeyValueListTransform
