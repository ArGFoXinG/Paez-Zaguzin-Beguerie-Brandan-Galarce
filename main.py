from gestionar_obras import GestionarObra
from modelo_orm2 import db, Obra # Asegúrate de importar 'db'
import sys
import datetime


def run_app():
    # Conectar a la base de datos y mapear ORM
    if not GestionarObra.conectar_db():
        print("No se pudo conectar a la base de datos. Saliendo.")
        sys.exit(1)

    if not GestionarObra.mapear_orm():
        print("No se pudieron mapear las tablas ORM. Saliendo.")
        db.close()
        sys.exit(1)

    try:
        if Obra.select().count() == 0:
            print("La tabla de obras está vacía. Cargando datos del CSV...")
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

        print("\n--- Demostración de Nuevas Obras y su Ciclo de Vida ---")

        # Obra de Ejemplo 1
        print("\nCreando y gestionando Obra de Ejemplo 1:")
        nueva_obra_1 = GestionarObra.nueva_obra()
        if nueva_obra_1:
            print(f"Obra '{nueva_obra_1.nombre}' iniciada como '{nueva_obra_1.etapa.nombre}'.")
            nueva_obra_1.iniciar_contratacion("LIC-2025-001", "Licitación Pública")
            nueva_obra_1.adjudicar_obra("Constructora Ejemplo S.A.", "20-12345678-9", "EXP-2025-001")
            nueva_obra_1.iniciar_obra(
                datetime.date(2025, 6, 1),
                datetime.date(2025, 12, 31),
                "Fondo Nacional",
                50
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
            nueva_obra_2.iniciar_contratacion("LIC-2025-002", "Licitación Privada")
            nueva_obra_2.adjudicar_obra("Constructora Dos S.A.", "20-98765432-1", "EXP-2025-002")
            nueva_obra_2.iniciar_obra(
                datetime.date(2025, 7, 1),
                datetime.date(2026, 1, 31),
                "Fondo Ciudad",
                30
            )
            nueva_obra_2.actualizar_porcentaje_avance(50.0)
            nueva_obra_2.aumentar_plazo(2)
            nueva_obra_2.incrementar_mano_obra(5)
            nueva_obra_2.actualizar_porcentaje_avance(100.0)
            nueva_obra_2.finalizar_obra()
            print(f"Obra '{nueva_obra_2.nombre}' (ID: {nueva_obra_2.id}) completó su ciclo de vida.")

        GestionarObra.obtener_indicadores()

    finally:
        if not db.is_closed():
            db.close()
            print("\nConexión a la base de datos cerrada.")

if __name__ == "__main__":
    run_app()