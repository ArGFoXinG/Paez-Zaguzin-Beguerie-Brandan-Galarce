from peewee import *
from datetime import date

db = SqliteDatabase('obras_urbanas.db')

class BaseModel(Model):
    class Meta:
        database = db

class Etapa(BaseModel):
    nombre = CharField(unique=True, null=False)

class TipoObra(BaseModel):
    nombre = CharField(unique=True, null=False)

class AreaResponsable(BaseModel):
    nombre = CharField(unique=True, null=False)

class Comuna(BaseModel):
    numero = IntegerField(unique=True, null=False)

class Barrio(BaseModel):
    nombre = CharField(null=False)
    comuna = ForeignKeyField(Comuna, backref='barrios')

class TipoContratacion(BaseModel):
    nombre = CharField(unique=True, null=False)

class Empresa(BaseModel):
    nombre = CharField(unique=True, null=False)

class Financiamiento(BaseModel):
    nombre = CharField(unique=True, null=False)

class Obra(BaseModel):
    nombre = CharField(null=False)
    descripcion = TextField(null=True)
    direccion = CharField(null=True)
    monto_contrato = FloatField(null=True)
    tipo_obra = ForeignKeyField('TipoObra', backref='obras', null=True)
    area_responsable = ForeignKeyField('AreaResponsable', backref='obras', null=True)
    etapa = ForeignKeyField('Etapa', backref='obras', null=True)
    comuna = ForeignKeyField('Comuna', backref='obras', null=True)
    barrio = ForeignKeyField('Barrio', backref='obras', null=True)
    porcentaje_avance = FloatField(null=True)
    fecha_inicio = DateField(null=True)
    fecha_fin_inicial = DateField(null=True)
    plazo_meses = IntegerField(null=True)
    mano_obra = IntegerField(null=True)
    contratacion_tipo = ForeignKeyField('TipoContratacion', backref='obras', null=True)
    nro_contratacion = CharField(null=True)
    licitacion_oferta_empresa = ForeignKeyField('Empresa', backref='obras', null=True)
    nro_expediente = CharField(null=True)
    financiamiento = ForeignKeyField('Financiamiento', backref='obras', null=True)
    destacada = CharField(null=True)

    class Meta:
        db_table = 'obras'

    # Métodos de instancia para el ciclo de vida
    def nuevo_proyecto(self):
        self.etapa, _ = Etapa.get_or_create(nombre="Proyecto")
        self.save()
        print(f"Obra '{self.nombre}' iniciada como 'Proyecto'.")

    def iniciar_contratacion(self, nro_contratacion_val, tipo_contratacion_nombre):
        self.etapa, _ = Etapa.get_or_create(nombre="Contratación")
        self.nro_contratacion = nro_contratacion_val
        self.contratacion_tipo, _ = TipoContratacion.get_or_create(nombre=tipo_contratacion_nombre)
        self.save()
        print(f"Obra '{self.nombre}': Iniciada Contratación ({self.nro_contratacion}, {self.contratacion_tipo.nombre}).")

    def adjudicar_obra(self, empresa_nombre, cuit_empresa, nro_expediente_val):
        self.etapa, _ = Etapa.get_or_create(nombre="Adjudicada")
        self.licitacion_oferta_empresa, _ = Empresa.get_or_create(nombre=empresa_nombre)
        self.nro_expediente = nro_expediente_val
        self.save()
        print(f"Obra '{self.nombre}': Adjudicada a {empresa_nombre} ({cuit_empresa}), Expediente: {nro_expediente_val}.")

    def iniciar_obra(self, fecha_inicio_val, fecha_fin_inicial_val, fuente_financiamiento_nombre, mano_obra_val):
        self.etapa, _ = Etapa.get_or_create(nombre="En ejecución")
        self.fecha_inicio = fecha_inicio_val
        self.fecha_fin_inicial = fecha_fin_inicial_val
        self.financiamiento, _ = Financiamiento.get_or_create(nombre=fuente_financiamiento_nombre)
        self.mano_obra = mano_obra_val
        self.save()
        print(f"Obra '{self.nombre}': Iniciada el {self.fecha_inicio}, Fin inicial: {self.fecha_fin_inicial}, Financiamiento: {self.financiamiento.nombre}, Mano de obra: {self.mano_obra}.")

    def actualizar_porcentaje_avance(self, nuevo_porcentaje):
        self.porcentaje_avance = nuevo_porcentaje
        self.save()
        print(f"Obra '{self.nombre}': Porcentaje de avance actualizado a {self.porcentaje_avance}%.")

    def aumentar_plazo(self, meses_extra):
        if self.plazo_meses is None:
            self.plazo_meses = 0
        self.plazo_meses += meses_extra
        self.save()
        print(f"Obra '{self.nombre}': Plazo incrementado en {meses_extra} meses (Total: {self.plazo_meses}).")

    def incrementar_mano_obra(self, cantidad_extra):
        if self.mano_obra is None:
            self.mano_obra = 0
        self.mano_obra += cantidad_extra
        self.save()
        print(f"Obra '{self.nombre}': Mano de obra incrementada en {cantidad_extra} (Total: {self.mano_obra}).")

    def finalizar_obra(self):
        self.etapa, _ = Etapa.get_or_create(nombre="Finalizada")
        self.porcentaje_avance = 100
        self.save()
        print(f"Obra '{self.nombre}': Finalizada.")

    def rescindir_obra(self):
        self.etapa, _ = Etapa.get_or_create(nombre="Rescindida")
        self.save()
        print(f"Obra '{self.nombre}': Rescindida.")