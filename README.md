# ecommerce_beam

Proyecto con datos ficticios (Faker) y Apache Beam.

El propósito es practicar el filtrado de información innecesaria, el enmascarado de datos sensibles y un poco de análisis.

Simula eventos de compras o intento de compras de un sistema ficticio de e-commerce.

# Faker

Genera datos relacionados al proyecto.

Usar

```python
python generator.py
```

Esto generará una estructura como

```
data/
 └── 2024/
      └── 03/
           └── 02/
                ├── part-0.parquet
```

# Apache Beam

Filtra, limpia, normaliza y carga en BD.
