def lambda_handler():
    from .funMetod06_Registro_Diario_Lluvias import *
    funRegistro_Diario_Lluvias()
    
    from .funMetod06_Indice_Anomalia_Lluvias import *
    funIndice_Anomalia_Lluvias()