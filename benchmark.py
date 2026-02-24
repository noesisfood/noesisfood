# benchmark.py

from typing import List, Dict

# -----------------------------
# Συνάρτηση ποινής θερμίδων
# -----------------------------
def calorie_penalty(product: Dict) -> int:
    calories = product.get("calories", 0)
    penalty = 0

    # Base penalty ανά calories
    if calories < 120:
        penalty = 0
    elif calories < 250:
        penalty = -5
    elif calories < 400:
        penalty = -10
    else:
        penalty = -15

    # Nutritional offsets
    offset = 0
    if product.get("fiber", 0) >= 3:
        offset += 3
    if product.get("protein", 0) >= 5:
        offset += 3
    if product.get("healthy_fats", False):
        offset += 2
    offset = min(offset, 5)

    # Liquid penalty
    if product.get("is_liquid", False):
        penalty -= product.get("liquid_penalty", 5)

    return penalty + offset

# -----------------------------
# Λίστα προϊόντων
# -----------------------------
benchmark_products: List[Dict] = [
    {"id":"1001","name":"Whole Wheat Bread","calories":110,"sugar_g":3,"fiber":5,"protein":4,"healthy_fats":False,"is_liquid":False,"processing":"minimally processed","allergens":["gluten"]},
    {"id":"1002","name":"Chocolate Breakfast Cereal","calories":200,"sugar_g":20,"fiber":2,"protein":3,"healthy_fats":False,"is_liquid":False,"processing":"ultra-processed","allergens":[]},
    {"id":"1003","name":"Coca-Cola 330ml","calories":139,"sugar_g":39,"fiber":0,"protein":0,"healthy_fats":False,"is_liquid":True,"liquid_penalty":10,"processing":"ultra-processed","allergens":[]},
    {"id":"1004","name":"Orange Juice 330ml","calories":150,"sugar_g":32,"fiber":1,"protein":1,"healthy_fats":False,"is_liquid":True,"liquid_penalty":5,"processing":"processed","allergens":[]},
    {"id":"1005","name":"Greek Yogurt 200g","calories":120,"sugar_g":6,"fiber":0,"protein":10,"healthy_fats":False,"is_liquid":False,"processing":"minimally processed","allergens":["milk"]},
    {"id":"1006","name":"Chocolate Bar 50g","calories":250,"sugar_g":25,"fiber":1,"protein":3,"healthy_fats":False,"is_liquid":False,"processing":"ultra-processed","allergens":["milk","nuts"]},
    {"id":"1007","name":"Almonds 30g","calories":170,"sugar_g":1,"fiber":3,"protein":6,"healthy_fats":True,"is_liquid":False,"processing":"minimally processed","allergens":["nuts"]},
    {"id":"1008","name":"Potato Chips 50g","calories":260,"sugar_g":0,"fiber":2,"protein":2,"healthy_fats":False,"is_liquid":False,"processing":"ultra-processed","allergens":[]},
    {"id":"1009","name":"Apple 150g","calories":80,"sugar_g":13,"fiber":3,"protein":0,"healthy_fats":False,"is_liquid":False,"processing":"natural","allergens":[]},
    {"id":"1010","name":"Banana 120g","calories":105,"sugar_g":12,"fiber":2,"protein":1,"healthy_fats":False,"is_liquid":False,"processing":"natural","allergens":[]},
    {"id":"1011","name":"Orange 150g","calories":70,"sugar_g":12,"fiber":3,"protein":1,"healthy_fats":False,"is_liquid":False,"processing":"natural","allergens":[]},
    {"id":"1012","name":"Ice Cream 100g","calories":207,"sugar_g":21,"fiber":0,"protein":3,"healthy_fats":False,"is_liquid":False,"processing":"ultra-processed","allergens":["milk"]},
    {"id":"1013","name":"Smoothie Berry 250ml","calories":180,"sugar_g":28,"fiber":2,"protein":1,"healthy_fats":False,"is_liquid":True,"liquid_penalty":3,"processing":"processed","allergens":[]},
    {"id":"1014","name":"Peanut Butter 30g","calories":190,"sugar_g":3,"fiber":2,"protein":7,"healthy_fats":True,"is_liquid":False,"processing":"minimally processed","allergens":["peanuts"]},
    {"id":"1015","name":"White Bread 50g","calories":130,"sugar_g":2,"fiber":1,"protein":3,"healthy_fats":False,"is_liquid":False,"processing":"processed","allergens":["gluten"]},
    {"id":"1016","name":"Cheddar Cheese 30g","calories":120,"sugar_g":0,"fiber":0,"protein":7,"healthy_fats":True,"is_liquid":False,"processing":"minimally processed","allergens":["milk"]},
    {"id":"1017","name":"Tomato 100g","calories":18,"sugar_g":2,"fiber":1,"protein":1,"healthy_fats":False,"is_liquid":False,"processing":"natural","allergens":[]},
    {"id":"1018","name":"Carrot 100g","calories":41,"sugar_g":5,"fiber":3,"protein":1,"healthy_fats":False,"is_liquid":False,"processing":"natural","allergens":[]},
    {"id":"1019","name":"Orange Juice with Sugar 330ml","calories":200,"sugar_g":38,"fiber":1,"protein":1,"healthy_fats":False,"is_liquid":True,"liquid_penalty":5,"processing":"ultra-processed","allergens":[]},
    {"id":"1020","name":"Energy Drink 250ml","calories":110,"sugar_g":27,"fiber":0,"protein":0,"healthy_fats":False,"is_liquid":True,"liquid_penalty":10,"processing":"ultra-processed","allergens":[]},
]

# -----------------------------
# Loop υπολογισμού VitaScore
# -----------------------------
def sugar_penalty(sugar: float) -> int:
    if sugar > 15:
        return 25
    elif sugar > 5:
        return 10
    return 0

for p in benchmark_products:
    score = 100

    # Ποινή ζάχαρης
    score -= sugar_penalty(p.get("sugar_g", 0))

    # Ποινή θερμίδων
    score += calorie_penalty(p)

    # Alerts
    alerts = []
    if p.get("sugar_g", 0) > 15:
        alerts.append("High sugar content")
    if score <= 70:
        alerts.append("Calorie-dense for its nutritional value")
    if p.get("is_liquid") and p.get("liquid_penalty", 0) > 0:
        alerts.append("Easy to overconsume calories")

    # Εμφάνιση
    print(f"--- {p['name']} ---")
    print(f"VitaScore: {score}")
    print("Alerts:", alerts if alerts else "No alerts")
    print()
