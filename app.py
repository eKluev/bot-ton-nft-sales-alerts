import save_data, send_alert


if __name__ == "__main__":
    save_data.Daemon('save_data',  30).start()
    send_alert.Daemon('send_alert', 10).start()