# "./datasets/tg_parsed_alarms.csv"


def upd_dataset(path,n_path):
    data = ["year,start_hour,start_min,duration_min,alarm_type\n"]
    with open(path,encoding="UTF-8") as file:
        # started,finished,duration_min,alarm_type
        file.readline()
        for line in file:
            l = line.strip().split(",")
            year = l[0][0:4]
            start_hour = l[0][11:13]
            start_min = l[0][14:16]
            duration_min = l[2]
            alarm_type = l[3]
            res = f"{year},{start_hour},{start_min},{duration_min},{alarm_type}\n"
            data.append(res)
    with open(n_path,mode="w",encoding='UTF-8') as file:
        # year,start_hour,start_min,duration_min,alarm_type
        for d in data:
            file.write(d)
