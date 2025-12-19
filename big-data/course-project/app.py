import joblib
import pandas as pd
import tkinter as tk
from tkinter import ttk

MODEL_PATH = "svm_heart_risk_pipeline.joblib"
THRESHOLD = 0.50
DEBOUNCE_MS = 350

SEX_OPTIONS = ["Male", "Female"]
CP_OPTIONS = ["typical angina", "atypical angina", "non-anginal", "asymptomatic"]
RESTECG_OPTIONS = ["normal", "st-t abnormality", "lv hypertrophy"]
SLOPE_OPTIONS = ["upsloping", "flat", "downsloping"]
BOOL_OPTIONS = ["False", "True"]

DEFAULTS = {
    "age": "35",
    "sex": "Male",
    "cp": "typical angina",
    "trestbps": "120",
    "chol": "190",
    "fbs": "False",
    "restecg": "normal",
    "thalch": "175",
    "exang": "False",
    "oldpeak": "0.0",
    "slope": "upsloping",
}

try:
    pipeline = joblib.load(MODEL_PATH)
except Exception as e:
    raise SystemExit(f"Помилка завантаження моделі: {e}")

def parse_float(s: str):
    s = (s or "").strip()
    try: return float(s) if s else None
    except ValueError: return None

def parse_bool(s: str) -> bool:
    return str(s).strip().lower() == "true"

def build_row(values: dict):
    age = parse_float(values["age"])
    trestbps = parse_float(values["trestbps"])
    chol = parse_float(values["chol"])
    thalch = parse_float(values["thalch"])
    oldpeak = parse_float(values["oldpeak"])

    if age is None or not (0 < age < 120): return None, "Вік: 1-119"
    if trestbps is None or trestbps <= 0: return None, "Тиск має бути > 0"
    if chol is None or chol <= 0: return None, "Холестерин має бути > 0"
    if thalch is None or thalch <= 0: return None, "Пульс має бути > 0"
    if oldpeak is None or oldpeak < 0: return None, "Oldpeak має бути ≥ 0"

    row = pd.DataFrame([{
        "age": age, "sex": values["sex"], "cp": values["cp"],
        "trestbps": trestbps, "chol": chol, "fbs": parse_bool(values["fbs"]),
        "restecg": values["restecg"], "thalch": thalch, "exang": parse_bool(values["exang"]),
        "oldpeak": oldpeak, "slope": values["slope"],
    }])
    return row, None

root = tk.Tk()
root.title("Аналіз серцевого ризику")
root.geometry("850x400")

main = ttk.Frame(root, padding=20)
main.pack(fill="both", expand=True)

left = ttk.Frame(main)
right = ttk.Frame(main)
left.pack(side="left", fill="both", expand=True)
right.pack(side="right", fill="y", padx=(20, 0))

ttk.Label(left, text="Клінічні показники", font=("Segoe UI", 14, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 15))

vars_ = {k: tk.StringVar(value=v) for k, v in DEFAULTS.items()}

def add_entry(row, label, key, unit=""):
    ttk.Label(left, text=label, width=45, anchor="w").grid(row=row, column=0, sticky="w", pady=3, padx=(0, 10))
    e = ttk.Entry(left, textvariable=vars_[key], width=12)
    e.grid(row=row, column=1, sticky="w", pady=3)
    if unit:
        ttk.Label(left, text=unit, foreground="gray").grid(row=row, column=1, sticky="e", padx=(85, 0))

def add_combo(row, label, key, options):
    ttk.Label(left, text=label, width=45, anchor="w").grid(row=row, column=0, sticky="w", pady=3, padx=(0, 10))
    cb = ttk.Combobox(left, textvariable=vars_[key], values=options, state="readonly", width=15)
    cb.grid(row=row, column=1, sticky="w", pady=3)

r = 1
add_entry(r, "Вік пацієнта", "age", "років"); r += 1
add_combo(r, "Стать (Sex)", "sex", SEX_OPTIONS); r += 1
add_combo(r, "Тип болю в грудях (Chest Pain)", "cp", CP_OPTIONS); r += 1
add_entry(r, "Артеріальний тиск у спокої", "trestbps", "mmHg"); r += 1
add_entry(r, "Рівень холестерину в сироватці", "chol", "mg/dL"); r += 1
add_entry(r, "Максимальний пульс (Thalch)", "thalch", "bpm"); r += 1
add_combo(r, "Рівень цукру натщесерце > 120 мг/дл", "fbs", BOOL_OPTIONS); r += 1
add_combo(r, "Результати ЕКГ у спокої", "restecg", RESTECG_OPTIONS); r += 1
add_combo(r, "Стенокардія, викликана вправами", "exang", BOOL_OPTIONS); r += 1
add_entry(r, "Депресія сегмента ST (Oldpeak)", "oldpeak"); r += 1
add_combo(r, "Нахил сегмента ST при піковому навантаженні", "slope", SLOPE_OPTIONS)

card = ttk.LabelFrame(right, text="Результат аналізу", padding=20)
card.pack(fill="both", expand=True)

risk_value = ttk.Label(card, text="—", font=("Segoe UI", 26, "bold"))
prob_value = ttk.Label(card, text="—", font=("Segoe UI", 16))
status_msg = ttk.Label(card, text="", foreground="darkorange", wraplength=200)

risk_value.pack(pady=(10, 5))
ttk.Label(card, text="Ймовірність патології:").pack()
prob_value.pack(pady=5)

bar = ttk.Progressbar(card, orient="horizontal", length=220, mode="determinate")
bar.pack(pady=20)
status_msg.pack()

after_id = None

def update_prediction(*_):
    global after_id
    if after_id: root.after_cancel(after_id)
    after_id = root.after(DEBOUNCE_MS, run_model)

def run_model():
    vals = {k: v.get() for k, v in vars_.items()}
    df_row, err = build_row(vals)
    if err:
        status_msg.config(text=err)
        return
    status_msg.config(text="")
    p = pipeline.predict_proba(df_row)[0, 1]
    pct = int(round(p * 100))
    prob_value.config(text=f"{pct}%")
    bar["value"] = pct
    if p >= THRESHOLD:
        risk_value.config(text="ВИСОКИЙ", foreground="#cc0000")
    else:
        risk_value.config(text="НИЗЬКИЙ", foreground="#008800")

for v in vars_.values():
    v.trace_add("write", update_prediction)

run_model()
root.mainloop()
