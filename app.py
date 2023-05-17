import save_data
import send_alert

if __name__ == "__main__":
    save_data.Daemon('save_data',  60).start()
    send_alert.Daemon('send_alert', 40).start()
