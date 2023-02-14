import haversine as hs
from haversine import Unit


def calculate_distance_pts(loc1, loc2, unit=Unit.METERS):
    """Calculate the great-circle distance between two points on the Earth surface.

    Takes two 2-tuples, containing the latitude and longitude of each point in decimal degrees,
    and, optionally, a unit of length.

    :param point1: first point; tuple of (latitude, longitude) in decimal degrees
    :param point2: second point; tuple of (latitude, longitude) in decimal degrees
    :param unit: a member of haversine.Unit, or, equivalently, a string containing the
                 initials of its corresponding unit of measurement (i.e. miles = mi)
                 default 'km' (kilometers).
    """
    return hs.haversine(loc1, loc2, unit)


def calculate_distance_vector(array1, array2, unit=Unit.METERS):
    """Calculate the great-circle distance between two points on the Earth surface.

    Takes two 2-tuples, containing the latitude and longitude of each point in decimal degrees,
    and, optionally, a unit of length.

    :param array1: array tuple of (latitude, longitude) in decimal degrees
    :param array2: array tuple of (latitude, longitude) in decimal degrees
    :param unit: a member of haversine.Unit, or, equivalently, a string containing the
                 initials of its corresponding unit of measurement (i.e. miles = mi)
                 default 'km' (kilometers).
    """
    return hs.haversine_vector(array1, array2, unit)
