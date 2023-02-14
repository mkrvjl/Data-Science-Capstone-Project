from datetime import time


def time_to_category(
    time_of_day: time,
) -> str:
    # define shift limits
    start_of_day = time(hour=0, minute=0)
    end_of_day = time(hour=23, minute=59, second=59, microsecond=999999)
    shift_end_evening = time(hour=0, minute=1)
    shift_end_night = time(hour=8, minute=1)
    shift_start_evening = time(hour=16, minute=1)

    # actual classifications happens here
    if start_of_day <= time_of_day < shift_end_evening:
        shift_category = 'soir'
    elif shift_end_evening <= time_of_day < shift_end_night:
        shift_category = 'nuit'
    elif shift_end_night <= time_of_day < shift_start_evening:
        shift_category = 'jour'
    elif shift_start_evening <= time_of_day < end_of_day:
        shift_category = 'soir'
    else:
        raise ValueError(f"Time '{time_of_day}' is invalid. Unable to classify!")

    # return category mapped
    return shift_category
