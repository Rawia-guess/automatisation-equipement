import os
import pandas as pd
from datetime import datetime
from netmiko import ConnectHandler
from netmiko.exceptions import NetmikoTimeoutException, NetmikoAuthenticationException





CSV_PATH = "equipement_reseau.csv"
OUTPUT_DIR = "outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def parse_commands(cmds: str):
    # Commands dans le CSV séparées par |
    if not isinstance(cmds, str) or not cmds.strip():
        return []
    return [c.strip() for c in cmds.split("|") if c.strip()]

def run():
    df = pd.read_csv(CSV_PATH, sep=";", dtype=str).fillna("")
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = []

    for _, row in df.iterrows():
        ticket = row.get("Ticket_ID", "").strip()
        ip = row.get("IP", "").strip()
        device_type = row.get("Device_Type", "").strip()
        username = row.get("Username", "").strip()
        password = row.get("Password", "").strip()
        secret = row.get("Enable_Secret", "").strip()
        action = row.get("Action", "").strip().upper()
        commands = parse_commands(row.get("Commands", ""))

        status = "OK"
        message = ""
        out_text = ""

        device = {
            "device_type": device_type,
            "host": ip,
            "username": username,
            "password": password,
            "secret": secret if secret else None,
        }

        try:
            conn = ConnectHandler(**device)

            # Si équipement nécessite enable
            if secret:
                conn.enable()

            if action == "AUDIT":
                # Exécute des show
                for cmd in commands:
                    out_text += f"\n### {cmd}\n"
                    out_text += conn.send_command(cmd, expect_string=None)

            elif action == "BACKUP":
                # Sauvegarde running-config (vendor dépendant, ici cisco ios)
                out_text = conn.send_command("show running-config")

            elif action == "PUSH":
                # Push commandes (config mode). Ici on suppose que la liste contient conf t, end etc.
                # Mieux: envoyer uniquement les lignes de config, Netmiko gère le mode config.
                # Donc si ton CSV met "conf t", "end", etc., on les filtre.
                config_lines = [c for c in commands if c.lower() not in ("conf t", "configure terminal", "end", "wr mem", "write memory")]
                out_text += conn.send_config_set(config_lines)

                # Sauvegreve si tu veux
                out_text += "\n### SAVE\n"
                out_text += conn.send_command("write memory")

            else:
                status = "SKIP"
                message = f"Action inconnue: {action}"

            conn.disconnect()

        except (NetmikoTimeoutException, NetmikoAuthenticationException) as e:
            status = "FAIL"
            message = f"{type(e).__name__}: {str(e)}"
        except Exception as e:
            status = "FAIL"
            message = f"ERROR: {type(e).__name__}: {str(e)}"

        # Sauvegarde output par ticket/IP
        safe_ticket = ticket if ticket else "NO_TICKET"
        file_name = f"{safe_ticket}_{ip}_{action}_{now}.txt".replace(":", "_")
        file_path = os.path.join(OUTPUT_DIR, file_name)
        save_text(file_path, out_text)

        results.append({
            "Ticket_ID": ticket,
            "IP": ip,
            "Action": action,
            "Status": status,
            "Message": message,
            "Output_File": file_path
        })

    # Export résumé CSV
    res_df = pd.DataFrame(results)
    summary_path = os.path.join(OUTPUT_DIR, f"summary_{now}.csv")
    res_df.to_csv(summary_path, index=False, sep=";", encoding="utf-8")

    print("✅ Terminé")
    print(f"- Résultats: {summary_path}")
    print(f"- Outputs:   {OUTPUT_DIR}/")

if __name__ == "__main__":
    run()
