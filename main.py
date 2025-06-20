# main.py

import pandas as pd
from gestionar_obras import GestionarObra
from modelo_orm import db, Obra # Asegúrate de importar 'db'
import sys
import datetime


def run_app():
    # Conectar a la base de datos y mapear ORM
    if not GestionarObra.conectar_db():
        print("No se pudo conectar a la base de datos. Saliendo.")
        sys.exit(1) # Salir si no se puede conectar

    if not GestionarObra.mapear_orm():
        print("No se pudieron mapear las tablas ORM. Saliendo.")
        db.close() # Asegurarse de cerrar la conexión si falla el mapeo
        sys.exit(1) # Salir si no se puede mapear


    try: # <--- AÑADIR ESTE TRY
        # Verificar si la tabla de obras está vacía
        if Obra.select().count() == 0:
            print("La tabla de obras está vacía. Cargando datos del CSV...")
            # Extraer, limpiar y cargar datos del CSV
            df = GestionarObra.extraer_datos()
            if df is not None:
                df_limpio = GestionarObra.limpiar_datos(df)
                if df_limpio is not None:
                    GestionarObra.cargar_datos(df_limpio)
                else:
                    print("La limpieza de datos falló. No se cargarán datos iniciales.")
            else:
                print("La extracción de datos del CSV falló. No se cargarán datos iniciales.")
        else:
            print("La tabla de obras ya contiene datos. Saltando la carga inicial del CSV.")

        # --- Demostración de Nuevas Obras y su Ciclo de Vida ---
        print("\n--- Demostración de Nuevas Obras y su Ciclo de Vida ---")

        # Obra de Ejemplo 1
        print("\nCreando y gestionando Obra de Ejemplo 1:")
        nueva_obra_1 = GestionarObra.nueva_obra()
        if nueva_obra_1:
            print(f"Obra '{nueva_obra_1.nombre}' iniciada como '{nueva_obra_1.etapa.nombre}'.")
            
            # Simulación del ciclo de vida de la obra (estos métodos están en Obra en modelo_orm.py)
        nueva_obra_1.iniciar_contratacion("LIC-2025-001", "Licitación Pública") # <--- CAMBIO AQUÍ
        nueva_obra_1.adjudicar_obra("Constructora Ejemplo S.A.", "20-12345678-9", "EXP-2025-001")
        nueva_obra_1.iniciar_obra(
            datetime.date(2025, 6, 1),      # fecha_inicio_val
            datetime.date(2025, 12, 31),    # fecha_fin_inicial_val (ejemplo)
            "Fondo Nacional",               # fuente_financiamiento_nombre (ejemplo)
            50                              # mano_obra_val (ejemplo)
        )
        nueva_obra_1.actualizar_porcentaje_avance(25.0)
        nueva_obra_1.aumentar_plazo(3)
        nueva_obra_1.incrementar_mano_obra(10)
        nueva_obra_1.actualizar_porcentaje_avance(75.0)
        nueva_obra_1.finalizar_obra()
        print(f"Obra '{nueva_obra_1.nombre}' (ID: {nueva_obra_1.id}) completó su ciclo de vida.")

        # Obra de Ejemplo 2
        print("\nCreando y gestionando Obra de Ejemplo 2:")
        nueva_obra_2 = GestionarObra.nueva_obra()
        if nueva_obra_2:
            print(f"Obra '{nueva_obra_2.nombre}' iniciada como '{nueva_obra_2.etapa.nombre}'.")
            # Podrías simular otro ciclo de vida aquí si lo deseas
            print(f"Obra '{nueva_obra_2.nombre}' (ID: {nueva_obra_2.id}) completó su ciclo de vida.")


        # --- Obtención de Indicadores ---
        GestionarObra.obtener_indicadores()

    finally: # <--- AÑADIR ESTE FINALLY
        # Cerrar la conexión a la base de datos al finalizar o si ocurre un error.
        if not db.is_closed():
            db.close()
            print("\nConexión a la base de datos cerrada.")


if __name__ == "__main__":
    run_app()