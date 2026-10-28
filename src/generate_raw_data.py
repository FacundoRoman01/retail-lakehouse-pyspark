import csv
import os
import random
from datetime import datetime, timedelta

# 1. Definimos la ruta de salida en la capa Bronze
output_dir = os.path.join("data", "bronze")
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "online_retail_raw.csv")

# 2. Catálogo local con precios en pesos argentinos (ARS) y categorías
products = [
    ("YER-001", "Yerba Mate Playadito 1kg", 4500.0, "Almacén"),
    ("CAF-002", "Café Molido Cabrales 500g", 6800.0, "Almacén"),
    ("TER-003", "Termo Stanley Classic 1L", 75000.0, "Bazar"),
    ("MAT-004", "Mate Imperial de Calabaza y Alpaca", 32000.0, "Bazar"),
    ("BOM-005", "Bombilla Pico de Loro Alpaca", 12500.0, "Bazar"),
    ("ALF-006", "Caja Alfajores Havanna Mixtos x12", 18500.0, "Golosinas"),
    ("DUL-007", "Dulce de Leche Chimbote 400g", 3900.0, "Almacén"),
    ("TAB-008", "Tabla de Algarrobo para Asado", 21000.0, "Hogar"),
    ("CUCH-009", "Cuchillo Artesanal Tandil Acero al Carbono", 28000.0, "Hogar"),
    ("VAS-010", "Vaso Térmico Acero Inoxidable 500ml", 16000.0, "Bazar"),
]

# Provincias para los envíos
provincias = [
    "Buenos Aires",
    "CABA",
    "Córdoba",
    "Santa Fe",
    "Mendoza",
    "Entre Ríos",
    "Tucumán"
]

fecha_inicio = datetime(2025, 1, 1)

print("Generando dataset crudo (Argentina) para capa Bronze...")

with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    
    # Encabezados de columnas
    writer.writerow([
        "id_factura",
        "codigo_producto",
        "descripcion",
        "cantidad",
        "fecha_factura",
        "precio_unitario",
        "id_cliente",
        "provincia",
        "categoria"
    ])

    for i in range(1, 15001):
        prod = random.choice(products)
        # Clientes identificados o nulos (compras como invitado)
        cliente = random.choice([f"CLI_{random.randint(1000, 1080)}", None])
        provincia = random.choice(provincias)
        
        # Fecha aleatoria en los primeros 6 meses
        dias_random = random.randint(0, 180)
        fecha = fecha_inicio + timedelta(days=dias_random, minutes=random.randint(0, 1440))
        
        # Suciedad simulada:
        # 1. 5% de devoluciones/anulaciones (cantidad negativa y factura con prefijo 'NC')
        if random.random() < 0.05:
            cantidad = -random.randint(1, 3)
            id_factura = f"NC-{100000 + i}"
        else:
            cantidad = random.randint(1, 12)
            id_factura = f"FC-{100000 + i}"

        # 2. 2% de errores de sistema con precio en cero
        precio_unitario = prod[2] if random.random() > 0.02 else 0.0

        writer.writerow([
            id_factura,
            prod[0],
            prod[1],
            cantidad,
            fecha.strftime("%Y-%m-%d %H:%M:%S"),
            precio_unitario,
            cliente if cliente else "",
            provincia,
            prod[3]
        ])

print(f"Dataset generado exitosamente en: {output_file}")