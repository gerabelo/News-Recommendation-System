#!/usr/bin/python
# -*- coding: utf-8 -*-

# Cajuideas 26/03/2018 Sistema de Recomendação de Notícias - Versão 1.0.0
#   a proximidade entre os usuarios se dará pela similaridade de consumo em grupo.
#   nesta versão o conteúdo individual consumido pertence a um grupo de tags similares.

import MySQLdb
import string
import math
import sys
import time
import re

from _mysql_exceptions import IntegrityError

tokenize = lambda doc: doc.lower().split(" ")

SourceTabela = "pages"
SourceUsuario = "sessions_id"
SourceConteudo = "title"
SourceDateIn = "date_in"
SourceDateOut = "date_out"
SourceUrl = "url"

Banco = "dbsessiondataservice"
BancoLocal = "localhost"
BancoUsuario = "root"
BancoSenha = ""

TabelaIndiceConteudo = "indicePTT"
TabelaIndiceTermos = "termos"
TabelaIndiceTags = "indiceTags"

TabelaSimilaridadeTitulos = "similaridadeTitulos"
TabelaSimilaridadeTags = "similaridadeTags"
TabelaSimilaridadeConsumidores = "similaridadeConsumidores"
TabelaSimilaridadeGruposConsumidores = "similaridadeGruposConsumidores"
#TabelaSimilaridadeGruposConteudos = "similaridadeGruposConteudos"
TabelaSimilaridadeGruposTitulos = "similaridadeGruposTitulos"
TabelaSimilaridadeGruposTags = "similaridadeGruposTags"

TabelaFiltragemColaborativa = "filtragemColaborativa"
TabelaConteudoStats = "contentStats"

TabelaGruposTags = "gruposTags"
TabelaGruposTitulos = "gruposTitulos"
TabelaGruposConsumidores = "gruposConsumidores"
TabelaGruposConsumidoresMedios = "gruposConsumidoresMedios"
TabelaGruposConteudosMedios = "gruposConteudosMedios" 
TabelaGruposTitulosMedios = "gruposTitulosMedios" 
TabelaGruposTagsMedios = "gruposTagsMedios" 
#TabelaGruposConteudos = TabelaGruposTags

TabelaAgrupamento = "agrupamento"
TabelaAgrupamentoTitulos = "agrupamentoTitulos"
TabelaConsumo = "consumo" #tags
TabelaConsumoTitulos = "consumoTitulos"

TabelaStopWords = "stopwords"
TabelaAnalysisTitulos = "analysisTitulos"
TabelaAnalysisTags = "analysisTags"

TabelaTermosTFIDFConteudo = "termosTFIDFConteudo"
TabelaTrendsTermosTFIDFTitulo = "trendsTermosTFIDFTitulo"
TabelaTrendsTitulosIDFMedio = "topTitulosTFIDFmedio"
TabelaTrendsTitulosVariancia = "topTitulosVariancia"

TabelaTermosTFIDFTitulo = "termosTFIDFTitulo"
TabelaTermosPorClassesGramaticais = "termos"

TabelagramaticalAnalysisTitulo = "gramaticaAnalysisTitulos"

TabelaTagsFrequencia = "tagsFrequencia"
TabelaTagsTrendsFrequencia = "tagsTrendsFrequencia"
TabelaTermosTitulosTrendsFrequencia = "termosTitulosTrendsFrequencia"
TabelaTermosConteudosTrendsFrequencia = "termosConteudosTrendsFrequencia"

vizinhancaTagsCorte = 0.1
vizinhancaTitulosCorte = 0.8
vizinhancaConsumidoresCorte = 0.8
maiorPalavra = 32
trendsLimit = 500


def jaccard_similarity_titulo(dataSet1, dataSet2):
    intersection = set(dataSet1).intersection(set(dataSet2))
    union = set(dataSet1).union(set(dataSet2))

    lenUnion = len(union)
    lenIntersection = len(intersection)

    if lenUnion > 0:
        return lenIntersection/lenUnion
    else:
        return 0.0

def tanimotoSimilarity_lista(dataSet1, dataSet2):
    return 0

def jaccardSimilarity_lista(dataSet1, dataSet2):
    lenUnion = 0
    try:
        dataSet1 = set(dataSet1.split(','))
        dataSet2 = set(dataSet2.split(','))

        intersection = set(dataSet1).intersection(set(dataSet2))
        union = set(dataSet1).union(set(dataSet2))
        
        lenUnion = len(union)
        lenIntersection = len(intersection)

    #    print('\n\ndataSet1: %s\ndataSet2: %s \nintersection: %s\nunion: %s' % (dataSet1,dataSet2,intersection,union))
    #    print('intersection: %s\nunion: %s\n%d/%d' % (intersection,union,lenIntersection,lenUnion))
    except:
        pass
    if lenUnion > 0:
        return lenIntersection/lenUnion
    else:
        return 0.0

def jaccard_similarity_numbers(dataSet1, dataSet2):
    resultado = 0.0
    try:
        set1 = str(dataSet1)
        set2 = str(dataSet2)
    
        set1 = set1.replace("(","")
        set1 = set1.replace(")","")
        set1 = set1.replace(",","")
        set1 = set1.replace("\'","")

        set2 = set2.replace("(","")
        set2 = set2.replace(")","")    
        set2 = set2.replace(",","")
        set2 = set2.replace("\'","")
        
        tamanho = len(set1)

        inter = 0
        uniao = 0
        x = 0
        
        while x < tamanho:
            if set1[x] != "0" and set2[x] != "0":
                if (abs(int(set1[x]) - int(set2[x]))) < 2:
                    inter = inter+1
                else:
                    uniao = uniao+2
            x=x+1

        if uniao > 0:
            resultado = inter/uniao
        else:
            resultado = 0.0
    except:
        pass
    return resultado

def inverse_document_frequencies(tokenized_documents):
    try:
        idf_values = {}
        all_tokens_set = set([item for sublist in tokenized_documents for item in sublist])
        for tkn in all_tokens_set:
            contains_token = map(lambda doc: tkn in doc, tokenized_documents)
            idf_values[tkn] = 1 + math.log(len(tokenized_documents)/(sum(contains_token)))
    except:
        pass
    return idf_values

def stemmingTags(dataSet):
    try:
        dataSet = dataSet.replace("[","")
        dataSet = dataSet.replace("]","")
        dataSet = dataSet.replace("\"","")
    except:
        pass
    return dataSet

def stemmingText(dataSet):
    try:
        dataSet = dataSet.replace("à","a")
        dataSet = dataSet.replace("á","a")
        dataSet = dataSet.replace("ã","a")
        dataSet = dataSet.replace("â","a")
        dataSet = dataSet.replace("é","e")
        dataSet = dataSet.replace("ê","e")
        dataSet = dataSet.replace("í","i")    
        dataSet = dataSet.replace("ó","o")
        dataSet = dataSet.replace("õ","o")
        dataSet = dataSet.replace("ô","o")
        dataSet = dataSet.replace("ú","u")
        dataSet = dataSet.replace("ü","u")
        dataSet = dataSet.replace("ç","c")
        dataSet = dataSet.replace(".","")
        dataSet = dataSet.replace(",","")
        dataSet = dataSet.replace("!","")
        dataSet = dataSet.replace("?","")
        dataSet = dataSet.replace("#","")
        dataSet = dataSet.replace("@","")
        dataSet = dataSet.replace("\'","")
        dataSet = dataSet.replace("\"","")
        dataSet = dataSet.replace("\\","")
        dataSet = dataSet.replace("/","")
        dataSet = dataSet.replace("_"," ")
        dataSet = dataSet.replace("-"," ")
        dataSet = dataSet.replace("+","")
        dataSet = dataSet.replace("(","")
        dataSet = dataSet.replace(")","")
        dataSet = dataSet.replace(":","")
        dataSet = dataSet.replace(";","")
        dataSet = dataSet.replace("{","")
        dataSet = dataSet.replace("}","")
        dataSet = dataSet.replace("|","")
        dataSet = dataSet.replace("[","")
        dataSet = dataSet.replace("]","")
        dataSet = dataSet.replace("=","")
        dataSet = dataSet.replace("+","")
        dataSet = dataSet.replace("^","")
        dataSet = dataSet.replace("&","")
        dataSet = dataSet.replace("%","")
        dataSet = dataSet.replace("<","")
        dataSet = dataSet.replace(">","")
        dataSet = dataSet.replace("  "," ")
        dataSet = dataSet.replace("\t","")
        dataSet = dataSet.replace("\r","")
        dataSet = dataSet.replace("\n","")
    except:
        pass
    return dataSet

def stemmingTitle(dataSet):
    try:
        dataSet = dataSet.replace("à","a")
        dataSet = dataSet.replace("á","a")
        dataSet = dataSet.replace("ã","a")
        dataSet = dataSet.replace("â","a")
        dataSet = dataSet.replace("é","e")
        dataSet = dataSet.replace("ê","e")
        dataSet = dataSet.replace("í","i")    
        dataSet = dataSet.replace("ó","o")
        dataSet = dataSet.replace("õ","o")
        dataSet = dataSet.replace("ô","o")
        dataSet = dataSet.replace("ú","u")
        dataSet = dataSet.replace("ü","u")
        dataSet = dataSet.replace("ç","c")
#        dataSet = dataSet.replace(".","")
#        dataSet = dataSet.replace(",","")
#        dataSet = dataSet.replace("!","")
#        dataSet = dataSet.replace("?","")
        dataSet = dataSet.replace("#","")
        dataSet = dataSet.replace("@","")
        dataSet = dataSet.replace("\'","")
        dataSet = dataSet.replace("\"","")
        dataSet = dataSet.replace("\\","")
        dataSet = dataSet.replace("/","")
        dataSet = dataSet.replace("_"," ")
        dataSet = dataSet.replace("-"," ")
        dataSet = dataSet.replace("+","")
        dataSet = dataSet.replace("(","")
        dataSet = dataSet.replace(")","")
        dataSet = dataSet.replace(":","")
        dataSet = dataSet.replace(";","")
        dataSet = dataSet.replace("{","")
        dataSet = dataSet.replace("}","")
        dataSet = dataSet.replace("|","")
        dataSet = dataSet.replace("[","")
        dataSet = dataSet.replace("]","")
        dataSet = dataSet.replace("=","")
        dataSet = dataSet.replace("+","")
        dataSet = dataSet.replace("^","")
        dataSet = dataSet.replace("&","")
        dataSet = dataSet.replace("%","")
        dataSet = dataSet.replace("<","")
        dataSet = dataSet.replace(">","")
        dataSet = dataSet.replace("  "," ")
        dataSet = dataSet.replace("\t","")
        dataSet = dataSet.replace("\r","")
        dataSet = dataSet.replace("\n","")
    except:
        pass
    return dataSet

def removestopwords(dataSet):
    dataSet = set(dataSet.split())
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "SELECT termo,status FROM %s;" % (TabelaIndiceTermos)
        cursor.execute(sql)
        termos = cursor.fetchall()
        qtd_termos = len(termos)
        x = 0
        while x < qtd_termos:
            if termos[x][0] in dataSet:
                if termos[x][1] in [1,2,3,4]:
                    pass #nao é stop word
                else:
                    dataSet.remove(termos[x][0])
                    #try:
                    #    dataSet.remove(termos[x][0])
                    #    #print(termos[x][0],termos[x][1])
                    #    sql = "INSERT IGNORE INTO stopwords (termo) VALUES ('%s');" % (termos[x][0])
                    #    cursor.execute(sql)
                    #except:
                    #    pass
            x += 1

        """
        if 'da' in dataSet: 
            dataSet.remove('da')
        if 'de' in dataSet: 
            dataSet.remove("de")
        if 'do' in dataSet:
            dataSet.remove("do")
        if 'a' in dataSet:
            dataSet.remove("a")
        if 'e' in dataSet:
            dataSet.remove("e")
        if 'o' in dataSet:
            dataSet.remove("o")
        if 'para' in dataSet: 
            dataSet.remove("para")
        if 'pelo' in dataSet:
            dataSet.remove("pelo")
        if 'por' in dataSet:
            dataSet.remove("por")
        if 'em' in dataSet: 
            dataSet.remove("em")
        if 'na' in dataSet: 
            dataSet.remove("na")
        if 'no' in dataSet: 
            dataSet.remove("no")
        if 'nem' in dataSet: 
            dataSet.remove("nem")
        if 'como' in dataSet: 
            dataSet.remove("como")
        if 'assim' in dataSet:
            dataSet.remove("assim")
        if 'nunca' in dataSet:
            dataSet.remove("nunca")            
        """
    except ValueError as e:
        print(e)
    db.close()
    return dataSet

def gerarTabelaStopWords():
    
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termo VARCHAR(64) UNIQUE, status INT(9) DEFAULT 0, PRIMARY KEY(id));" % (TabelaStopWords,TabelaStopWords)
    sql = "SELECT titulo FROM indiceptt;"
    cursor.execute(sql)

    titulos = cursor.fetchall()
    qtd_titulos = len(titulos)

    x=0
    while x < qtd_titulos:
        removestopwords(titulos[x][0])
        x +=1
    
    db.close()
    return 1

###########################################################################################################
# indexarPaginasTitulos - gera uma Tabela de índice para os artigos (url e titulo)                        #
###########################################################################################################
def indexarConteudo():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, url VARCHAR(256) UNIQUE, titulo VARCHAR(512), tags VARCHAR(512), PRIMARY KEY(id));" % (TabelaIndiceConteudo,TabelaIndiceConteudo)
        print(cursor.execute(sql))
        sql = "SELECT url,title,tags FROM %s;" % (SourceTabela)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            #try:
            sql = "INSERT IGNORE INTO %s (url,titulo,tags) VALUES ('%s','%s','%s');" % (TabelaIndiceConteudo,row[0],stemmingTitle(row[1]),row[2])
                #print(sql2)
            cursor.execute(sql)
            #except ValueError as e:
            #    print(sql2)
            #    print("Unexpected error:", sys.exc_info()[0])
            #    print(e)
            #    db.close()
            #    return -1
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        print(sql)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

###########################################################################################################
# filtragemColaborativa - gera uma matriz utilidade com repetições                                        #
###########################################################################################################
def filtragemColaborativa():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, session_id INT(9) NOT NULL, content_id INT(9) NOT NULL, rating INT(1), PRIMARY KEY(id));" % (TabelaFiltragemColaborativa,TabelaFiltragemColaborativa)
        print(cursor.execute(sql))
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    
    postContentTitles = []
    postContentIds = [] #paginas
    postSessions = [] #usuarios
    postRatings = []

    sql = "SELECT %s.id, %s.%s, %s.%s, timestampdiff(SECOND,%s.%s,%s.%s) FROM %s INNER JOIN %s ON %s.%s = %s.url;" % (TabelaIndiceConteudo, SourceTabela, SourceConteudo, SourceTabela, SourceUsuario, SourceTabela, SourceDateIn, SourceTabela, SourceDateOut, SourceTabela, TabelaIndiceConteudo, SourceTabela, SourceUrl, TabelaIndiceConteudo)

    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        #print('Total de Registros: %d' % (len(results)))
        for row in results:
            if len(row[1]):
                x = int(math.log(1+pow(row[3],2),7))
                if x < 5:
                    postRatings.append(x)
                else:
                    postRatings.append(5)
                postSessions.append(row[2])
                postContentTitles.append(stemmingText(row[1]))
                postContentIds.append(row[0])
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        print(row[3])
        db.close()
        return -1

    numberOfTitles = len(postRatings)
    #jaccards = [[0 for x in range(numberOfTitles)] for y in range(numberOfTitles)]
    k = 0
    while (k < numberOfTitles):
        try:
            sql = "INSERT IGNORE INTO %s (session_id,content_id,rating) VALUES ('%d','%d','%d');" % (TabelaFiltragemColaborativa, postSessions[k], postContentIds[k],postRatings[k])
            cursor.execute(sql)
        except ValueError as e:
            print("Unexpected error:", sys.exc_info()[0],e)
            db.close()
        k = k + 1
    db.commit()
    db.close()
    return 1

def agruparConteudosPorTags():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId INT(9) UNIQUE NOT NULL, grupoId INT(9) NOT NULL, PRIMARY KEY(id));" % (TabelaGruposTags,TabelaGruposTags)
        print(cursor.execute(sql))
        sql = "SELECT id FROM %s;" % (TabelaIndiceConteudo)
        cursor.execute(sql)
        conteudos = cursor.fetchall()
        for conteudo in conteudos:
            # 1.0 P: o conteudo já está em algum grupo? se estiver, faz nada. apenas ignora e segue o jogo.
            sql = "SELECT id FROM %s WHERE conteudoId = %d;" % (TabelaGruposTags,conteudo[0])
            cursor.execute(sql)
            busca = len(cursor.fetchall())
            if busca == 0:
                # 1.0 R: nao
                # Selecionar o conteudo mais proximo e agrupar
                sql = "SELECT conteudoId2 FROM %s WHERE conteudoId2 <> %d AND conteudoId1 = %d AND similaridade > %d ORDER BY similaridade DESC LIMIT 1;" % (TabelaSimilaridadeTags,conteudo[0],conteudo[0],vizinhancaTagsCorte)
                #sql = "SELECT conteudoId2 FROM %s WHERE conteudoId2 <> %d AND conteudoId1 = %d ORDER BY similaridade DESC LIMIT 1;" % (TabelaSimilaridadeTags,conteudo[0],conteudo[0])
                cursor.execute(sql)
                vizinho = cursor.fetchall()
                # 1.1 P: encontrou vizinho?
                if len(vizinho) > 0:
                    # qual o grupoTagsId do vizinho?
                    sql = "SELECT grupoId FROM %s WHERE conteudoId = %d;" % (TabelaGruposTags,vizinho[0][0])
                    cursor.execute(sql)
                    grupo = cursor.fetchall()
                    if len(grupo) > 0:
                        sql = "INSERT INTO %s (conteudoId,grupoId) VALUES (%d,%d);" % (TabelaGruposTags,conteudo[0],grupo[0][0])
                        cursor.execute(sql)
                    else:
                        sql = "SELECT grupoId FROM %s ORDER BY grupoId DESC LIMIT 1;" % (TabelaGruposTags)
                        cursor.execute(sql)
                        grupoLast = cursor.fetchall()
                        if len(grupoLast) == 0:
                            grupoNew = 1
                        else:
                            grupoNew = grupoLast[0][0]+1
                        sql = "INSERT INTO %s (conteudoId,grupoId) VALUES (%d,%d);" % (TabelaGruposTags,conteudo[0],grupoNew)
                        cursor.execute(sql)
                else:
                    #pegar ultimo grupo, incrementar e criar.
                    sql = "SELECT grupoId FROM %s ORDER BY grupoId DESC LIMIT 1;" % (TabelaGruposTags)
                    cursor.execute(sql)
                    grupoLast = cursor.fetchall()
                    if len(grupoLast) == 0:
                        grupoNew = 1
                    else:
                        grupoNew = grupoLast[0][0]+1
                    sql = "INSERT INTO %s (conteudoId,grupoId) VALUES (%d,%d);" % (TabelaGruposTags,conteudo[0],grupoNew)
                    cursor.execute(sql)
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1 
    db.commit()
    db.close()
    return 1

def agruparConteudosPorTitulos():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId INT(9) NOT NULL, grupoId INT(9) NOT NULL, PRIMARY KEY(id));" % (TabelaGruposTitulos,TabelaGruposTitulos)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT id FROM %s;" % (TabelaIndiceConteudo)
        cursor.execute(sql)
        conteudos = cursor.fetchall()
        for conteudo in conteudos:
            sql = "SELECT id FROM %s WHERE conteudoId = %d;" % (TabelaGruposTitulos,conteudo[0])
            cursor.execute(sql)
            busca = len(cursor.fetchall())
            if busca == 0:
                sql = "SELECT conteudoId2 FROM %s WHERE conteudoId2 <> %d AND conteudoId1 = %d AND similaridade > %d ORDER BY similaridade DESC LIMIT 1;" % (TabelaSimilaridadeTitulos,conteudo[0],conteudo[0],vizinhancaTitulosCorte)
                cursor.execute(sql)
                vizinho = cursor.fetchall()
                if len(vizinho) > 0:
                    sql = "SELECT grupoId FROM %s WHERE conteudoId = %d;" % (TabelaGruposTitulos,vizinho[0][0])
                    cursor.execute(sql)
                    grupo = cursor.fetchall()
                    if len(grupo) > 0:
                        sql = "INSERT INTO %s (conteudoId,grupoId) VALUES (%d,%d);" % (TabelaGruposTitulos,conteudo[0],grupo[0][0])
                        cursor.execute(sql)
                    else:
                        sql = "SELECT grupoId FROM %s ORDER BY grupoId DESC LIMIT 1;" % (TabelaGruposTitulos)
                        cursor.execute(sql)
                        grupoLast = cursor.fetchall()
                        if len(grupoLast) == 0:
                            grupoNew = 1
                        else:
                            grupoNew = grupoLast[0][0]+1
                        sql = "INSERT INTO %s (conteudoId,grupoId) VALUES (%d,%d);" % (TabelaGruposTitulos,conteudo[0],grupoNew)
                        cursor.execute(sql)                        
                else:
                    #pegar ultimo grupo, incrementar e criar.
                    sql = "SELECT grupoId FROM %s ORDER BY grupoId DESC LIMIT 1;" % (TabelaGruposTitulos)
                    cursor.execute(sql)
                    grupoLast = cursor.fetchall()
                    if len(grupoLast) == 0:
                        grupoNew = 1
                    else:
                        grupoNew = grupoLast[0][0]+1
                    sql = "INSERT INTO %s (conteudoId,grupoId) VALUES (%d,%d);" % (TabelaGruposTitulos,conteudo[0],grupoNew)
                    cursor.execute(sql)

    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1 
    db.commit()
    db.close()   
    return 1


############################################################################################################
# PRE AGRUPAMENTO - Gera a Tabela 'agrupamento' contendo a matriz utilidade final (filtragem colaborativa) #
###################################################################################################K########
def gerarTabelaDeGruposDeConteudosConsumidosPorConsumidor():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    try:
        """
        # versao 1
        sql = "SELECT COUNT(*) FROM %s;" % (TabelaIndiceConteudo) #id, url, titulo, tags
        cursor.execute(sql)
        results = cursor.fetchall()
        totalPaginas = results[0][0]
        #sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId INT(9) NOT NULL, consumo VARCHAR(%d) NOT NULL, PRIMARY KEY(id));" % (TabelaAgrupamento,TabelaAgrupamento,totalPaginas)
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId INT(9) NOT NULL, grupos VARCHAR(%d) NOT NULL, PRIMARY KEY(id));" % (TabelaAgrupamento,TabelaAgrupamento,totalPaginas)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT session_id FROM %s;" % (TabelaFiltragemColaborativa) # session_id, content_id, rating
        cursor.execute(sql)
        usuarios = cursor.fetchall()
        for usuario in usuarios:
            #pegar os ratings do usuário para cada pagina
            sql = "SELECT id FROM %s;" % (TabelaIndiceConteudo) # id, url, titulos, tags                
            cursor.execute(sql)
            pgs = cursor.fetchall()
            #ratings = [] # ???
            grupos = []
            for pg in pgs:
                sql = "SELECT rating FROM %s WHERE session_id=%d AND content_id=%d ORDER BY rating DESC LIMIT 1;" % (TabelaFiltragemColaborativa, usuario[0],pg[0])
                # pegou todos os ratings do usuário corrente
                cursor.execute(sql)
                r = cursor.fetchall()
                if len(r) > 0:
                    if r[0][0] > 1: #condição inserida em 02/04/2018: permaneceu mais que 7 segundos no site
                        # Existe um Rating logo visitou a páginas
                        sql = "SELECT grupoId FROM %s WHERE conteudoId = %d;" % (TabelaGruposTags,pg[0])
                        # pegou o grupo da página visitada
                        cursor.execute(sql)
                        grupo = cursor.fetchall()
                        if len(grupo) > 0:
                            if grupo[0][0] in grupos:
                                pass
                            else:
                                grupos.append(grupo[0][0])
            gruposToStr = str(grupos)
            sql = "INSERT IGNORE INTO %s (consumidorId,grupos) VALUES (%d,'%s');" % (TabelaAgrupamento,usuario[0],gruposToStr)
            cursor.execute(sql)
        """        
        #versao 2 07/04/2018
        sql = "SELECT COUNT(*) FROM %s;" % (TabelaIndiceConteudo) #id, url, titulo, tags
        cursor.execute(sql)
        results = cursor.fetchall()
        totalPaginas = results[0][0]
        #sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId INT(9) NOT NULL, consumo VARCHAR(%d) NOT NULL, PRIMARY KEY(id));" % (TabelaAgrupamento,TabelaAgrupamento,totalPaginas)
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId INT(9) NOT NULL, grupos VARCHAR(%d) NOT NULL, PRIMARY KEY(id));" % (TabelaAgrupamento,TabelaAgrupamento,totalPaginas)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT session_id FROM %s;" % (TabelaFiltragemColaborativa) # session_id, content_id, rating
        cursor.execute(sql)
        usuarios = cursor.fetchall()
        for usuario in usuarios:
            sql = "SELECT rating,content_id FROM %s WHERE session_id=%d GROUP BY content_id ORDER BY rating DESC;" % (TabelaFiltragemColaborativa, usuario[0])
            # pegou todos os ratings do usuário corrente
            cursor.execute(sql)
            r = cursor.fetchall()
            #ratings = [] # ???
            grupos = []
            for pontuacao in r:
                if pontuacao[0] > 1: #condição inserida em 02/04/2018: permaneceu mais que 7 segundos no site
                    # Existe um Rating logo visitou a página. (tem que estar) Ela está em algum grupo que possa ser inserido como consumo do usuário?
                    sql = "SELECT grupoId FROM %s WHERE conteudoId = %d;" % (TabelaGruposTags,pontuacao[1]) #pontuacao[1] = Id do conteudo
                    
                    # retornou o grupoIdTag da página visitada
                    cursor.execute(sql)
                    grupo = cursor.fetchall()
                    if len(grupo) > 0:
                        if grupo[0][0] in grupos:
                            pass #grupo já na lista
                        else:
                            grupos.append(grupo[0][0])
                    else:
                        print("conteudo sem grupo: ",pontuacao[1])
                        grupos.append(0)
                    
            gruposToStr = str(grupos)
            sql = "INSERT IGNORE INTO %s (consumidorId,grupos) VALUES (%d,'%s');" % (TabelaAgrupamento,usuario[0],gruposToStr)
            cursor.execute(sql)
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaDeGruposDeConteudosPorTituloConsumidosPorConsumidor():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    try:       
        #versao 2 07/04/2018
        sql = "SELECT COUNT(*) FROM %s;" % (TabelaIndiceConteudo) #id, url, titulo, tags
        cursor.execute(sql)
        results = cursor.fetchall()
        totalPaginas = results[0][0]
        
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId INT(9) NOT NULL, grupos VARCHAR(%d) NOT NULL, PRIMARY KEY(id));" % (TabelaAgrupamentoTitulos,TabelaAgrupamentoTitulos,totalPaginas)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT session_id FROM %s;" % (TabelaFiltragemColaborativa) # session_id, content_id, rating
        cursor.execute(sql)
        usuarios = cursor.fetchall()
        for usuario in usuarios:
            sql = "SELECT rating,content_id FROM %s WHERE session_id=%d GROUP BY content_id ORDER BY rating DESC;" % (TabelaFiltragemColaborativa, usuario[0])
            # pegou todos os ratings do usuário corrente
            cursor.execute(sql)
            r = cursor.fetchall()
            #ratings = [] # ???
            grupos = []
            for pontuacao in r:
                if pontuacao[0] > 1: #condição inserida em 02/04/2018: permaneceu mais que 7 segundos no site
                    # Existe um Rating logo visitou a páginas
                    sql = "SELECT grupoId FROM %s WHERE conteudoId = %d;" % (TabelaGruposTitulos,pontuacao[1])
                    # pegou o grupo da página visitada
                    cursor.execute(sql)
                    grupo = cursor.fetchall()
                    if len(grupo) > 0:
                        if grupo[0][0] in grupos:
                            pass #grupo já na lista
                        else:
                            grupos.append(grupo[0][0])
                    else:
                        print("conteudo sem grupo: ",pontuacao[1])
                        grupos.append(0)
                    
            gruposToStr = str(grupos)
            sql = "INSERT IGNORE INTO %s (consumidorId,grupos) VALUES (%d,'%s');" % (TabelaAgrupamentoTitulos,usuario[0],gruposToStr)
            cursor.execute(sql)
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaSimilaridadeTitulos():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        #sql = "CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId1 INT(9) NOT NULL, conteudoId2 INT(9) NOT NULL, similaridade VARCHAR(18) NOT NULL, PRIMARY KEY(id));" % (TabelaSimilaridadeTitulos,TabelaSimilaridadeTitulos)
        #print(cursor.execute(sql))

        sql = "SELECT id,titulo FROM %s;" % (TabelaIndiceConteudo)
        cursor.execute(sql)
        conteudo = cursor.fetchall()
        totalConteudo = len(conteudo)

        i = 0
        while i < totalConteudo:
            j = 0
            db.commit()
            while j < totalConteudo:
                conjunto1 = removestopwords(conteudo[i][1])
                conjunto2 = removestopwords(conteudo[j][1])
                similaridade = jaccard_similarity_titulo(conjunto1,conjunto2)
                #print(conjunto1,":",conjunto2,":",similaridade)
                #time.sleep(1)
                sql = "INSERT IGNORE INTO %s (conteudoId1,conteudoId2,similaridade) VALUES (%d,%d,'%.18f');" % (TabelaSimilaridadeTitulos,conteudo[i][0],conteudo[j][0],similaridade)
                #print(sql)
                cursor.execute(sql)
                j += 1
            i += 1
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaSimilaridadeTags():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId1 INT(9) NOT NULL, conteudoId2 INT(9) NOT NULL, similaridade VARCHAR(18) NOT NULL, PRIMARY KEY(id));" % (TabelaSimilaridadeTags,TabelaSimilaridadeTags)
        print(cursor.execute(sql))

        sql = "SELECT id,tags FROM %s;" % (TabelaIndiceConteudo)
        cursor.execute(sql)
        conteudo = cursor.fetchall()
        totalConteudo = len(conteudo)

        i = 0
        while i < totalConteudo:
            j = 0
            while j < totalConteudo:
                similaridade = jaccardSimilarity_lista(stemmingTags(conteudo[i][1]),stemmingTags(conteudo[j][1]))
                #print(similaridade)
                #time.sleep(1)
                sql = "INSERT IGNORE INTO %s (conteudoId1,conteudoId2,similaridade) VALUES (%d,%d,'%.18f');" % (TabelaSimilaridadeTags,conteudo[i][0],conteudo[j][0],similaridade)
                #print(sql)
                cursor.execute(sql)
                j = j+1
            i = i+1
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def syllable_count(word):
    count = 0
    try:
        word = word.lower()
        vowels = "aeiouy"
        if word[0] in vowels:
            count += 1
        for index in range(1, len(word)):
            if word[index] in vowels and word[index - 1] not in vowels:
                count += 1
                #if word.endswith("e"):
                #    count -= 1
        if count == 0:
            count += 1
    # ditongos e tritongos nao se separam!
        if "ie" in word:
            count -= 1
        if "io" in word:
            count -= 1
        if "oa" in word:
            count -= 1
        if "ie" in word:
            count -= 1
        if "ua" in word:
            count -= 1
        if "ea" in word:
            count -= 1
        if "eo" in word:
            count -= 1
        if "ou" in word:
            count -= 1
        if "ai" in word:
            count -= 1
        if "au" in word:
            count -= 1
        if "ui" in word:
            count -= 1
        if "ão" in word:
            count -= 1
        if "ãe" in word:
            count -= 1
        if "uê" in word:
            count -= 1
        if "ue" in word:
            count -= 1
        if "uai" in word:
            count -= 1
        if "uão" in word:
            count -= 1
        if "uei" in word:
            count -= 1
    except:
        pass
    return count
    
def Flesch_Kincaid_readingEase(dataSet):
    #legibilidade
    result = 0
    quantidadePalavras = contarPalavras(dataSet)

    quantidadeSilabas = 0.0
    text = dataSet.split(" ")
    x = 0
    while x < len(text):
        quantidadeSilabas += syllable_count(text[x])
        x += 1

    #result = (206.835-(1.015*(quantidadePalavras/contarSentencas(dataSet)))-(84.6*(quantidadeSilabas/quantidadePalavras)))
    result = (236.835-(1.015*(quantidadePalavras/contarSentencas(dataSet)))-(84.6*(quantidadeSilabas/quantidadePalavras)))

    return result

def Flesch_Kincaid_gradeLevel(dataSet):
    #legibilidade
    result = 0
    quantidadePalavras = contarPalavras(dataSet)
    
    quantidadeSilabas = 0.0
    text = dataSet.split(" ")
    x = 0
    while x < len(text):
        quantidadeSilabas += syllable_count(text[x])
        x += 1

    result = ((0.39*(quantidadePalavras/contarSentencas(dataSet)))+(11.8*(quantidadeSilabas/quantidadePalavras)))-15.59

    return result


def contarSentencas(texto):
    resultado = len(re.split('[.,!?]',texto))
    return resultado

def contarPalavras(frase):
    resultado = 0
    frase = frase.split(" ")
    resultado = len(frase)
    return resultado

def contarCaracteres(palavra):
    resultado = 0
    resultado = len(palavra)    
    return resultado

def palavrasTamanhoMedio(dataSet):
    words = dataSet.split()
    return sum(len(word) for word in words) / len(words)


def analysisTitulos():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId INT(9), FK_RE VARCHAR(18), FK_GL VARCHAR(18), Gunning_Fog INT(3), qtd_Palavras INT(9), qtd_Sentencas INT(9), palavrasTamanhoMedio INT(9), qtd_Stopwords INT(9), PRIMARY KEY (id));" % (TabelaAnalysisTitulos,TabelaAnalysisTitulos)
    print(cursor.execute(sql))

    sql = "SELECT a.id, a.titulo, b.mediarating FROM indiceptt a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM contentStats a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM gruposTags a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM gruposTags a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (trendsLimit)
    cursor.execute(sql)
    results = cursor.fetchall()

    for row in results:
        FK_RE = Flesch_Kincaid_readingEase(row[1])
        FK_GL = Flesch_Kincaid_gradeLevel(row[1])
        qtd_palavras = contarPalavras(row[1])
        qtd_sentencas = contarSentencas(row[1])
        palavrasTMedio = palavrasTamanhoMedio(row[1])
        sql = "INSERT IGNORE INTO %s (conteudoId,FK_RE,FK_GL,qtd_palavras,qtd_sentencas,palavrasTamanhoMedio) VALUES (%d,'%.18f','%.18f',%d,%d,%d);" % (TabelaAnalysisTitulos,row[0],FK_RE,FK_GL,qtd_palavras,qtd_sentencas,palavrasTMedio)
        cursor.execute(sql)
    db.commit()
    db.close()
    return 0

def gerarTabelaTermosConteudosTrendsFrequencia():
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termoId INT(9) UNIQUE, contador INT(12) DEFAULT 1, PRIMARY KEY(id));" % (TabelaTermosConteudosTrendsFrequencia,TabelaTermosConteudosTrendsFrequencia)
        print(cursor.execute(sql))

        sql = "SELECT a.content FROM %s a JOIN (SELECT a.url, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC) b ON b.url = a.url;" % (SourceTabela,TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        conteudosSet = cursor.fetchall()
        qtd_conteudos = len(conteudosSet)

        sql = "SELECT id, termo FROM %s;" % (TabelaTermosPorClassesGramaticais)
        cursor.execute(sql)
        termos = cursor.fetchall()
        qtd_termos = len(termos)

        x = 0
        while x < qtd_conteudos:
            conteudos_por_artigo = conteudosSet[x][0]
            conteudos_por_artigo = conteudos_por_artigo.replace("\"","")
            conteudos_por_artigo = conteudos_por_artigo.replace("[","")
            conteudos_por_artigo = conteudos_por_artigo.replace("]","")
            conteudos_por_artigo = conteudos_por_artigo.replace("?","")
            conteudos_por_artigo = conteudos_por_artigo.replace("!","")
            conteudos_por_artigo = conteudos_por_artigo.replace(",","")
            conteudos_por_artigo = conteudos_por_artigo.replace(".","")
            conteudos_por_artigo = conteudos_por_artigo.replace("\n","")
            conteudos_por_artigo = conteudos_por_artigo.split(" ")
            
            for termo in conteudos_por_artigo:
                y = 0
                while y < qtd_termos:
                    if termos[y][1] == termo:
                        termoId = termos[y][0]
                        sql = "INSERT INTO %s (termoId) VALUES (%d) ON DUPLICATE KEY UPDATE contador=contador+1;" % (TabelaTermosConteudosTrendsFrequencia,termoId)
                        cursor.execute(sql)
                        break
                    y += 1
            x += 1
    except ValueError as e:
        print(e)
    db.commit()
    db.close()
    return 0

def gerarTabelaTermosTitulosTrendsFrequencia():
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termoId INT(9) UNIQUE, contador INT(12) DEFAULT 1, PRIMARY KEY(id));" % (TabelaTermosTitulosTrendsFrequencia,TabelaTermosTitulosTrendsFrequencia)
        print(cursor.execute(sql))

        sql = "SELECT a.titulo, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        titulosSet = cursor.fetchall()
        qtd_titulos = len(titulosSet)
        
        sql = "SELECT id, termo FROM %s;" % (TabelaTermosPorClassesGramaticais)
        cursor.execute(sql)
        termos = cursor.fetchall()
        qtd_termos = len(termos)

        x = 0
        while x < qtd_titulos:
            titulos_por_artigo = titulosSet[x][0]
            titulos_por_artigo = titulos_por_artigo.replace("\"","")
            titulos_por_artigo = titulos_por_artigo.replace("[","")
            titulos_por_artigo = titulos_por_artigo.replace("]","")
            titulos_por_artigo = titulos_por_artigo.replace("?","")
            titulos_por_artigo = titulos_por_artigo.replace("!","")
            titulos_por_artigo = titulos_por_artigo.replace(",","")
            titulos_por_artigo = titulos_por_artigo.replace(".","")
            titulos_por_artigo = titulos_por_artigo.replace("\n","")
            titulos_por_artigo = titulos_por_artigo.split(" ")
            
            for termo in titulos_por_artigo:
                try:
                    y = 0
                    while y < qtd_termos:
                        if termos[y][1] == termo:
                            termoId = termos[y][0]
                            sql = "INSERT INTO %s (termoId) VALUES ('%s') ON DUPLICATE KEY UPDATE contador=contador+1;" % (TabelaTermosTitulosTrendsFrequencia,termoId)
                            cursor.execute(sql)
                            break
                        y += 1
                except:
                    pass
            x += 1
    except ValueError as e:
        print(e)
    db.commit()
    db.close()
    return 0
    
def conteudoStats():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s;  CREATE TABLE %s (id INT(9) AUTO_INCREMENT, content_id INT(9) NOT NULL, acessos INT(9), mediarating INT(9), PRIMARY KEY(id));" % (TabelaConteudoStats,TabelaConteudoStats)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT content_id FROM %s;" % (TabelaFiltragemColaborativa)
        # pegou todas as páginas
        cursor.execute(sql)
        results = cursor.fetchall()
        totalConteudo = len(results)
        print(totalConteudo)
    
        i = 0
        while i < totalConteudo:
            sql = "SELECT rating FROM %s WHERE content_id = %d;" % (TabelaFiltragemColaborativa,results[i][0])
            # pegou todos os ratings para a pagina corrente
            cursor.execute(sql)
            ratings = cursor.fetchall()
            numerodeacessos = len(ratings)
            mediarating = 0.0
            #media dos ratings
            j = 0
            while j < numerodeacessos:
                mediarating = mediarating+ratings[j][0]
                j = j+1
            mediarating = mediarating/numerodeacessos
            sql = "INSERT IGNORE INTO %s (content_id, acessos, mediarating) VALUES (%d,%d,%d);" % (TabelaConteudoStats,results[i][0],numerodeacessos,mediarating)
            cursor.execute(sql)
            i = i+1
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def encontraVizinhoMaisProximo(pessoa):
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    winner = 0
    similar = 0
    
    try:
        sql = "SELECT grupos, consumidorId FROM %s;" % (TabelaAgrupamento)
        cursor.execute(sql)
        consumidores = cursor.fetchall()
        totalConsumidores = len(consumidores)
        x = 0
        while x < totalConsumidores:
            if consumidores[x][1] == pessoa:
                y = 0
                while y < totalConsumidores:
                    if (consumidores[y][1] != pessoa):
                        if (len(consumidores[y][0]) > 0):
                            temp = jaccardSimilarity_lista(stemmingTags(consumidores[x][0]),stemmingTags(consumidores[y][0]))
                            if similar < temp and temp > vizinhancaConsumidoresCorte:
                                winner = consumidores[y][1]
                                similar = temp
                    #print(candidatos[x][0],consumidor)
                    y += 1
                break
            x += 1

        
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()
    return winner

def gruposConsumidores():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId INT(9) NOT NULL, grupoId INT(9) NOT NULL, PRIMARY KEY(id));" % (TabelaGruposConsumidores,TabelaGruposConsumidores)
        cursor.execute(sql)
        sql = "SELECT DISTINCT session_id FROM %s;" % (TabelaFiltragemColaborativa)
        cursor.execute(sql)
        users = cursor.fetchall()
        totalConsumidores = len(users)
        x=0        
        while (x < totalConsumidores):
            neighbor = 0            
            neighbor = encontraVizinhoMaisProximo(users[x][0])
            
            if (neighbor != 0):
                sql = "SELECT grupoId FROM %s WHERE consumidorId = %d;" % (TabelaGruposConsumidores,neighbor)
                cursor.execute(sql)
                group = cursor.fetchall()
                if (len(group) > 0):
                    sql = "INSERT IGNORE INTO %s (consumidorId,grupoId) VALUES (%d,%d);" % (TabelaGruposConsumidores,users[x][0],group[0][0])
                    cursor.execute(sql)
                else:
                    sql = "SELECT grupoId FROM %s ORDER BY grupoId DESC LIMIT 1;" % (TabelaGruposConsumidores)
                    cursor.execute(sql)
                    grupoLast = cursor.fetchall()
                    if (len(grupoLast) == 0):
                        grupoNew = 1
                    else:
                        grupoNew = grupoLast[0][0]+1
                    sql = "INSERT IGNORE INTO %s (consumidorId,grupoId) VALUES (%d,%d);" % (TabelaGruposConsumidores,users[x][0],grupoNew)
                    cursor.execute(sql)
            x += 1
    except ValueError as e:
        print("Unexpected error:", sys.exc_info()[0],e)
        db.close()
        return -1
    db.commit()
    db.close()   
    return 1

def gerarTabelaSimilaridadeConsumidoresPorGruposDeConteudosConsumidos():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, consumidorId1 INT(9), consumidorId2 INT(9), similaridade VARCHAR(18), PRIMARY KEY(id));" % (TabelaSimilaridadeConsumidores,TabelaSimilaridadeConsumidores)
        cursor.execute(sql)
        sql = "SELECT consumidorId, grupos FROM %s;" % (TabelaAgrupamento)
        cursor.execute(sql)
        consumidores = cursor.fetchall()
        totalConsumidores = len(consumidores)
        
        x = 0
        while x < totalConsumidores:
            y = 0
            similaridade = 0.0
            while y < totalConsumidores:
                if consumidores[x][0] != consumidores[y][0]:
                    similaridade = jaccardSimilarity_lista(stemmingTags(consumidores[x][1]),stemmingTags(consumidores[y][1]))
#                if similaridade > 0.0: # desempenho. nao insere similaridades nulas
                    sql = "INSERT IGNORE INTO %s (consumidorId1,consumidorId2,similaridade) VALUES (%d,%d,%.18f);" % (TabelaSimilaridadeConsumidores,consumidores[x][0],consumidores[y][0],similaridade)
                    cursor.execute(sql)
                y += 1
            x += 1
        """
        # gera a Tabela com o par mais proximo
        x = 0
        while x < totalConsumidores:
            y = 0
            similaridade = 0
            vencedor = 0
            while y < totalConsumidores:
                if consumidores[x][0] != consumidores[y][0]:
                    similaridadeTemp = jaccard_similarity_tags(stemmingTags(consumidores[x][1]),stemmingTags(consumidores[y][1]))
                    if similaridadeTemp > similaridade:
                        similaridade = similaridadeTemp
                        vencedor = consumidores[y][0]
                y += 1
            if vencedor > 0:
                sql = "INSERT IGNORE INTO %s (consumidorId1,consumidorId2,similaridade) VALUES (%d,%d,%.18f);" % (TabelaSimilaridadeConsumidores,consumidores[x][0],vencedor,similaridade)
                cursor.execute(sql)
            x += 1
        """
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1
    
def consumidorMedioPorGrupo(grupoId):
    # retorna o consumidorId com maior similaridade media
    representante = 0
    maiorSimilaridadeMedia = 0
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        sql = "SELECT a.consumidorId,a.grupos FROM %s a JOIN %s b ON a.consumidorId = b.consumidorId WHERE b.grupoId = %d" % (TabelaAgrupamento, TabelaGruposConsumidores, grupoId)
        cursor.execute(sql)
        consumidoresDoGrupo = cursor.fetchall()
        totalConsumidoresDoGrupo = len(consumidoresDoGrupo)
        x = 0
        while x < totalConsumidoresDoGrupo:
            y = 0
            similaridadeMedia = 0.0
            while y < totalConsumidoresDoGrupo:
                #if consumidoresDoGrupo[x][0] != consumidoresDoGrupo[y][0]:
                similaridadeMedia += float(jaccardSimilarity_lista(stemmingTags(consumidoresDoGrupo[x][1]),stemmingTags(consumidoresDoGrupo[y][1])))
                y += 1
            similaridadeMedia = similaridadeMedia/totalConsumidoresDoGrupo
            if similaridadeMedia > maiorSimilaridadeMedia:
                maiorSimilaridadeMedia = similaridadeMedia
                representante = consumidoresDoGrupo[x][0]
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return representante

def conteudoMedioPorGrupo(grupoId):
    # retorna o conteudoId com maior similaridade media
    representante = 0
    maiorSimilaridade = 0
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "SELECT conteudoId FROM %s WHERE grupoId = %d;" % (TabelaGruposTags,grupoId)
        cursor.execute(sql)
        conteudoDoGrupo = cursor.fetchall()
        totalConteudoDoGrupo = len(conteudoDoGrupo)
        x = 0
        while x < totalConteudoDoGrupo:
            sql = "SELECT similaridade,conteudoId2 FROM %s WHERE conteudoId1 = %d;" % (TabelaSimilaridadeTags,conteudoDoGrupo[x][0])
            cursor.execute(sql)
            similaridades = cursor.fetchall()
            similaridadeMedia = 0.0
            for similaridade in similaridades:
                #print(conteudoDoGrupo,similaridade[1])
                if similaridade[1] in conteudoDoGrupo[0]:
                    similaridadeMedia += float(similaridade[0])
            #similaridadeMedia = similaridadeMedia/len(similaridades)
            similaridadeMedia = similaridadeMedia/totalConteudoDoGrupo
            if similaridadeMedia > maiorSimilaridade:
                maiorSimilaridade = similaridadeMedia
                representante = conteudoDoGrupo[x][0]
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return representante

def gerarTabelaSimilaridadeGruposConsumidores():
    # TabelaGruposConsumidoresMedios TabelaAgrupamento
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoId1 INT(9), grupoId2 INT(9), similaridade VARCHAR(18), PRIMARY KEY(id));" % (TabelaSimilaridadeGruposConsumidores,TabelaSimilaridadeGruposConsumidores)
        print(cursor.execute(sql))

        sql = "SELECT a.grupoId, a.consumidorId, b.grupos from %s a JOIN %s b ON b.consumidorId = a.consumidorId;" % (TabelaGruposConsumidoresMedios,TabelaAgrupamento)
        cursor.execute(sql)
        consumidoresMedios = cursor.fetchall()
        totalGrupos = len(consumidoresMedios)
        x = 0
        while x < totalGrupos:
            y = 0
            while y < totalGrupos:
                #similaridade grupos[x][0] grupos[y][0]
                similaridade = jaccardSimilarity_lista(stemmingTags(consumidoresMedios[x][2]),stemmingTags(consumidoresMedios[y][2]))
                sql = "INSERT IGNORE INTO %s (grupoId1,grupoId2,similaridade) VALUES (%d,%d,%d);" % (TabelaSimilaridadeGruposConsumidores,consumidoresMedios[x][0],consumidoresMedios[y][0],similaridade)
                cursor.execute(sql)
                y += 1
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaSimilaridadeGruposTags():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoId1 INT(9), grupoId2 INT(9), similaridade VARCHAR(18), PRIMARY KEY(id));" % (TabelaSimilaridadeGruposTags,TabelaSimilaridadeGruposTags)
        print(cursor.execute(sql))

        sql = "SELECT grupoId, conteudoId from %s;" % (TabelaGruposTagsMedios)
        cursor.execute(sql)
        grupoConteudosMedios = cursor.fetchall()
        
        totalGrupos = len(grupoConteudosMedios)
        x = 0
        while x < totalGrupos:
            y = 0
            while y < totalGrupos:
                sql = "SELECT similaridade from %s WHERE conteudoId1 = %d AND conteudoId2 = %d;" % (TabelaSimilaridadeTags,grupoConteudosMedios[x][1],grupoConteudosMedios[y][1])
                cursor.execute(sql)
                similaridade = cursor.fetchall()
                sql = "INSERT IGNORE INTO %s (grupoId1,grupoId2,similaridade) VALUES (%d,%d,%s);" % (TabelaSimilaridadeGruposTags,grupoConteudosMedios[x][0],grupoConteudosMedios[y][0],similaridade[0][0])
                cursor.execute(sql)
                y += 1
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaSimilaridadeGruposTitulos():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoId1 INT(9), grupoId2 INT(9), similaridade VARCHAR(18), PRIMARY KEY(id));" % (TabelaSimilaridadeGruposTitulos,TabelaSimilaridadeGruposTitulos)
        print(cursor.execute(sql))

        sql = "SELECT grupoId, conteudoId from %s;" % (TabelaGruposTitulosMedios)
        cursor.execute(sql)
        grupoConteudosMedios = cursor.fetchall()
        
        totalGrupos = len(grupoConteudosMedios)
        x = 0
        while x < totalGrupos:
            y = 0
            while y < totalGrupos:
                sql = "SELECT similaridade from %s WHERE conteudoId1 = %d AND conteudoId2 = %d;" % (TabelaSimilaridadeTitulos,grupoConteudosMedios[x][1],grupoConteudosMedios[y][1])
                cursor.execute(sql)
                similaridade = cursor.fetchall()
                sql = "INSERT IGNORE INTO %s (grupoId1,grupoId2,similaridade) VALUES (%d,%d,%s);" % (TabelaSimilaridadeGruposTitulos,grupoConteudosMedios[x][0],grupoConteudosMedios[y][0],similaridade[0][0])
                cursor.execute(sql)
                y += 1
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaConsumidoresMediosPorGrupo():
    #total grupos consumidores
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoId INT(9) NOT NULL, consumidorId INT(9), PRIMARY KEY(id));" % (TabelaGruposConsumidoresMedios,TabelaGruposConsumidoresMedios)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT grupoId FROM %s;" % (TabelaGruposConsumidores)
        cursor.execute(sql)
        gruposConsumidores = cursor.fetchall()
        totalGruposConsumidores = len(gruposConsumidores)
        x = 0
        while x < totalGruposConsumidores:
            # encontrar consumidor medio por grupo
            consumidorMedio = consumidorMedioPorGrupo(gruposConsumidores[x][0])
            sql = "INSERT IGNORE INTO %s (grupoId,consumidorId) VALUES (%d,%d);" % (TabelaGruposConsumidoresMedios,gruposConsumidores[x][0],consumidorMedio)
            cursor.execute(sql)
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaTitulosMediosPorGrupo():
    #total grupos conteudos
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoId INT(9) NOT NULL, conteudoId INT(9), PRIMARY KEY(id));" % (TabelaGruposTitulosMedios,TabelaGruposTitulosMedios)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT grupoId FROM %s;" % (TabelaGruposTitulos)
        cursor.execute(sql)
        gruposConteudos = cursor.fetchall()
        totalGruposConteudos = len(gruposConteudos)

        x = 0
        while x < totalGruposConteudos:
            # encontrar consumidor medio por grupo
            conteudoMedio = conteudoMedioPorGrupo(gruposConteudos[x][0])
            sql = "INSERT IGNORE INTO %s (grupoId,conteudoId) VALUES (%d,%d);" % (TabelaGruposTitulosMedios,gruposConteudos[x][0],conteudoMedio)
            cursor.execute(sql)
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaTagsMediosPorGrupo():
    #total grupos conteudos
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoId INT(9) NOT NULL, conteudoId INT(9), PRIMARY KEY(id));" % (TabelaGruposTagsMedios,TabelaGruposTagsMedios)
        print(cursor.execute(sql))
        sql = "SELECT DISTINCT grupoId FROM %s;" % (TabelaGruposTags)
        cursor.execute(sql)
        gruposConteudos = cursor.fetchall()
        totalGruposConteudos = len(gruposConteudos)

        x = 0
        while x < totalGruposConteudos:
            # encontrar consumidor medio por grupo
            conteudoMedio = conteudoMedioPorGrupo(gruposConteudos[x][0])
            sql = "INSERT IGNORE INTO %s (grupoId,conteudoId) VALUES (%d,%d);" % (TabelaGruposTagsMedios,gruposConteudos[x][0],conteudoMedio)
            cursor.execute(sql)
            x += 1
    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 1

def gerarTabelaIndiceTermos():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        #sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termo VARCHAR(%d) UNIQUE, tf INT(9), idf INT(9), status INT(9), PRIMARY KEY(id));" % (TabelaIndiceTermos,TabelaIndiceTermos,maiorPalavra)
        #print(cursor.execute(sql))

        sql = "SELECT id,content FROM %s ORDER BY id DESC;" % (SourceTabela)
        cursor.execute(sql)
        paginas = cursor.fetchall()
        totalPaginas = len(paginas)

        wordSet = {}
        x = 0
        #print(totalPaginas)
        #while totalPaginas >= 0:
        while x < totalPaginas:
            #print(len(paginas[x][1]))
            #wordSet = stemmingText(paginas[x][1].lower()).split(" ")
            wordSet = paginas[x][1].lower().split(" ")
            #totalWords = len(wordSet)
            #print(wordSet)
            #y = 0
            #while y < totalWords:
            for word in wordSet:
                sql = "INSERT IGNORE INTO %s (termo) VALUES ('%s');" % (TabelaIndiceTermos,word)
                cursor.execute(sql)
                time.sleep(0.05)
                #print(sql)
                #y += 1
            #totalPaginas -= 1  
            x += 1
    except:
        pass
    db.commit()
    db.close()
    return 1
    
def termosTFIDFConteudo():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termo VARCHAR(32) UNIQUE, IDF VARCHAR(18), TF VARCHAR(18), status INT(9), PRIMARY KEY (id));" % (TabelaTermosTFIDFConteudo,TabelaTermosTFIDFConteudo)
        print(cursor.execute(sql))
        sql = "SELECT content FROM %s;" % (SourceTabela)
        cursor.execute(sql)
        topTitulos = cursor.fetchall()
        qtd_topTitulos = len(topTitulos)
        qtd_termosNaFrase = 0
        x = 0
        while x < qtd_topTitulos:
            #titulos
            y = 0
            frase = topTitulos[x][0]
            termos = frase.split(" ")
            qtd_termosNaFrase = contarPalavras(frase)
            while y < qtd_termosNaFrase:
                #termos
                j = 0                
                termo = termos[y]
                i = 0
                ocorrencias = 0
                qtd_termos = 0
                while i < qtd_topTitulos:
                    ocorrencias += topTitulos[i][0].count(termo)
                    qtd_termos += len(topTitulos[i][0].split(" "))
                    if ocorrencias > 0:
                        j += 1
                    i += 1
                try:
                    sql = "INSERT IGNORE INTO %s (termo,IDF,TF) VALUES ('%s','%.18f','%.18f');" % (TabelaTermosTFIDFConteudo,termo,(math.log(qtd_topTitulos/j)),(ocorrencias/qtd_termos))
                    cursor.execute(sql)
                except:
                    pass
                y += 1
            x += 1
    except ValueError as e:
        print(e)
        return -1
    db.commit()
    db.close()
    return 0

def termosTFIDFTitulo():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termoId INT(9) UNIQUE, IDF VARCHAR(18), TF VARCHAR(18), status INT(9), PRIMARY KEY (id));" % (TabelaTermosTFIDFTitulo,TabelaTermosTFIDFTitulo)
        print(cursor.execute(sql))
        sql = "SELECT titulo FROM %s;" % (TabelaIndiceConteudo)
        cursor.execute(sql)
        topTitulos = cursor.fetchall()
        qtd_topTitulos = len(topTitulos)

        sql = "SELECT id,termo FROM %s;" % (TabelaTermosPorClassesGramaticais)
        cursor.execute(sql)
        termosIndice = cursor.fetchall()
        qtd_termosIndice = len(termosIndice)

        qtd_termosNaFrase = 0
        x = 0
        while x < qtd_topTitulos:
            #titulos
            y = 0
            frase = topTitulos[x][0]
            termos = frase.split(" ")
            qtd_termosNaFrase = contarPalavras(frase)
            while y < qtd_termosNaFrase:
                #termos
                j = 0                
                termo = termos[y]
                i = 0
                ocorrencias = 0
                qtd_termos = 0
                while i < qtd_topTitulos:
                    ocorrencias += topTitulos[i][0].count(termo)
                    qtd_termos += len(topTitulos[i][0].split(" "))
                    if ocorrencias > 0:
                        j += 1
                    i += 1
                try:
                    k = 0
                    while k < qtd_termosIndice:
                        if termosIndice[k][1] == termo:
                            sql = "INSERT IGNORE INTO %s (termoId,IDF,TF) VALUES ('%d','%.18f','%.18f');" % (TabelaTermosTFIDFTitulo,termosIndice[k][0],(math.log(qtd_topTitulos/j)),(ocorrencias/qtd_termos))
                            cursor.execute(sql)
                            break
                        k += 1
                except:
                    pass
                y += 1
            x += 1
    except ValueError as e:
        print(e)
        return -1
    db.commit()
    db.close()
    return 0

def trendsTermosTFIDFTitulo():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termoId INT(9) UNIQUE, IDF VARCHAR(18), TF VARCHAR(18), status INT(9), PRIMARY KEY (id));" % (TabelaTrendsTermosTFIDFTitulo,TabelaTrendsTermosTFIDFTitulo)
        print(cursor.execute(sql))
        sql = "SELECT a.titulo, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        topTitulos = cursor.fetchall()
        qtd_topTitulos = len(topTitulos)

        qtd_termosNaFrase = 0
        x = 0
        while x < qtd_topTitulos:
            #titulos 
            y = 0
            frase = topTitulos[x][0]
            termos = frase.split(" ")
            qtd_termosNaFrase = contarPalavras(frase)
            while y < qtd_termosNaFrase:
                #termos
                #verificar classe gramatical
                termo = termos[y]
                termo = termo.replace(",","")
                termo = termo.replace(".","")
                termo = termo.replace("?","")
                termo = termo.replace("!","")
                termo = termo.replace(" ","")
                termo = termo.replace("\n","")
                sql = "SELECT status,id FROM %s WHERE termo = '%s';" % (TabelaTermosPorClassesGramaticais,termo)
                cursor.execute(sql)
                gramaticaTermo = cursor.fetchall()

                j = 0                
                i = 0
                ocorrencias = 0
                qtd_termos = 0
                while i < qtd_topTitulos:
                    ocorrencias += topTitulos[i][0].count(termo)
                    qtd_termos += len(topTitulos[i][0].split(" "))
                    if ocorrencias > 0:
                        j += 1
                    i += 1
                if len(gramaticaTermo) > 0:
                    if gramaticaTermo[0][0] in [0,1,2,3,4]:        
                        sql = "INSERT IGNORE INTO %s (termoId,IDF,TF,status) VALUES ('%d','%.18f','%.18f',%d);" % (TabelaTrendsTermosTFIDFTitulo,gramaticaTermo[0][1],(math.log(qtd_topTitulos/(1+j))),(ocorrencias/(1+qtd_termos)),gramaticaTermo[0][0])
                        #cursor.execute(sql)
                cursor.execute(sql)
                y += 1
            x += 1
    except ValueError as e:
        print(e)
        return -1
    db.commit()
    db.close()
    return 0

def calcularIDFmedio(frase):
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    resultado = 0.0

    try:
        sql = "SELECT termo, idf FROM %s;" % (TabelaTrendsTermosTFIDFTitulo)
        cursor.execute(sql)
        resultados = cursor.fetchall()
        qtd_termosNaBase = len(resultados)
        termos = frase.split(" ")
        qtd_termosNaFrase = len(termos)
        for termo in termos:
            x = 0
            while x < qtd_termosNaBase:
                if resultados[x][0] == termo:
                    resultado += float(resultados[x][1])
                    break
                x += 1            
        resultado = resultado/qtd_termosNaFrase

    except ValueError as e:
        print(e)
        db.close()
        return 0.0
    db.commit()
    db.close()
    return resultado

def calcularTFmedio(frase):
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    resultado = 0.0

    try:
        sql = "SELECT termo, tf FROM %s;" % (TabelaTrendsTermosTFIDFTitulo)
        cursor.execute(sql)
        resultados = cursor.fetchall()
        qtd_termosNaBase = len(resultados)
        termos = frase.split(" ")
        qtd_termosNaFrase = len(termos)
        for termo in termos:
            x = 0
            while x < qtd_termosNaBase:
                if resultados[x][0] == termo:
                    resultado += float(resultados[x][1])
                    break
                x += 1            
        resultado = resultado/qtd_termosNaFrase

    except ValueError as e:
        print(e)
        db.close()
        return 0.0
    db.commit()
    db.close()
    return resultado

def topTitulosIDFmedio():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    try:
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId INT(9) UNIQUE, tfmedio VARCHAR(18), idfmedio VARCHAR(18), PRIMARY KEY(id));" % (TabelaTrendsTitulosIDFMedio,TabelaTrendsTitulosIDFMedio)
        print(cursor.execute(sql))

        sql = "SELECT a.id, a.titulo, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        resultados = cursor.fetchall()
        qtd_topTitulos = len(resultados)

        x = 0
        while x < qtd_topTitulos:
            sql = "INSERT IGNORE INTO %s (conteudoId,idfmedio,tfmedio) VALUES (%d,'%.18f','%.18f');" % (TabelaTrendsTitulosIDFMedio,resultados[x][0],calcularIDFmedio(resultados[x][1]),calcularTFmedio(resultados[x][1]))
            cursor.execute(sql)
            x += 1        

    except ValueError as e:
        print(e)
        db.close()
        return -1
    db.commit()
    db.close()
    return 0

def recomendacaoBayes(entradaTags):
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
	
    try:
        # tags dos conteudosId medios / representantes dos grupos conteudos
        sql = "SELECT a.id, a.tags, b.grupoId FROM %s a JOIN (SELECT a.conteudoId, a.grupoId FROM %s a) b ON a.id = b.conteudoId;" % (TabelaIndiceConteudo,TabelaGruposTagsMedios)
        cursor.execute(sql)
        representantes = cursor.fetchall()
        totalrepresentantes = len(representantes)

        recomendacao = 0
        probabilidadeCondicional = 0.0
        maiorProbabilidadeCondicional = 0.0
        grupo = 0
        similaridade = 0.0
        i = 0
        while i < totalrepresentantes:
            similaridadeTemp = jaccardSimilarity_lista(stemmingTags(entradaTags),stemmingTags(representantes[i][1]))
            if similaridade < similaridadeTemp:
                similaridade = similaridadeTemp
                grupo = representantes[i][2]
            i += 1

        sql = "SELECT a.id, a.tags, b.grupoId FROM %s a JOIN (SELECT a.conteudoId, a.grupoId FROM %s a WHERE a.grupoId = %d) b ON a.id = b.conteudoId;" % (TabelaIndiceConteudo,TabelaGruposTags,grupo)
        cursor.execute(sql)
        candidatos = cursor.fetchall()
        totalCandidatos = len(candidatos)
        x = 0
        while x < totalCandidatos:
            y = 0
            d = 0
            n = 0
            while y < totalCandidatos:
                candidatoTags = candidatos[x][1]
                #match 100% ou maior similaridade Jaccard?
                if entradaTags in candidatos[y][1]:
                    d += 1
                    if candidatoTags in candidatos[y][1]:
                        n += 1
                y += 1
            x += 1
            probabilidadeCondicional = n/d
            print(candidatos[x][0],probabilidadeCondicional)
            if probabilidadeCondicional > maiorProbabilidadeCondicional:
                maiorProbabilidadeCondicional = probabilidadeCondicional
                recomendacao = candidatos[x][0] 
    except:
        pass
        db.close()
        return 0
    db.commit()
    db.close()
    return recomendacao

def desviopadrao(dataSet):
    resultado = 0.0
    try:
        n = len(dataSet)
        u = 0.0

        i = 0
        while i < n:
            u += dataSet[i]
            i += 1
        u = u/n

        i = 0
        while i < n:
            resultado += math.pow((dataSet[i]-u),2)
            i += 1
        resultado = math.sqrt((resultado/(n-1)))
    except:
        pass
    return resultado

def variancia(dataSet):
    resultado = 0.0
    try:
        n = len(dataSet)
        u = 0.0

        i = 0
        while i < n:
            u += dataSet[i]
            i += 1
        u = u/n

        i = 0
        while i < n:
            resultado += math.pow((dataSet[i]-u),2)
            i += 1
        resultado = resultado/(n-1)
    except:
        pass
    return resultado

def covariancia(dataSet1,dataSet2):
    resultado = 0.0
    try:
        n = len(dataSet1)
        if len(dataSet2) != n:
            return resultado
        u1 = 0.0
        u2 = 0.0
        i = 0
        while i < n:
            u1 += dataSet1[i]
            u2 += dataSet2[i]
            i += 1
        u1 = u1/n
        u2 = u2/n

        i = 0
        while i < n:
            resultado = (dataSet1-u1)*(dataSet2-u2)
            i += 1
        resultado = resultado/(n-1)
    except:
        pass
    return resultado

def pearsonCorrelation(dataSet1,dataSet2):
    resultado = 0.0
    try:
        resultado = covariancia(dataSet1,dataSet2)/math.sqrt(variancia(dataSet1)*variancia(dataSet2))
    except:
        pass
    return resultado

def tituloTrendsVariancia():    
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()
        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId INT(9) UNIQUE, variancia VARCHAR(18), PRIMARY KEY(id));" % (TabelaTrendsTitulosVariancia,TabelaTrendsTitulosVariancia)
        print(cursor.execute(sql))

        sql = "SELECT a.id, a.titulo, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        resultados = cursor.fetchall()
        qtd_topTitulos = len(resultados)

        sql = "SELECT termo, tf, idf FROM %s;" % (TabelaTrendsTermosTFIDFTitulo)
        cursor.execute(sql)
        tfidf = cursor.fetchall()
        qtd_tfidf = len(tfidf)

        x = 0
        #print("quantidade top titulos: ",qtd_topTitulos)
        while x < qtd_topTitulos:
            #frase
            titulo = resultados[x][1].split(" ")
            #print("titulo: ",titulo)
            words = len(titulo)
            relevancia = []
            y = 0
            while y < words:
                #palavra
                z = 0
                while z < qtd_tfidf:
                    #print("qtd_tfidf: ",qtd_tfidf)
                    #print(tfidf[z][1],tfidf[z][2])
                    if tfidf[z][0] == titulo[y]:
                        relevancia.append(float(tfidf[z][1])*float(tfidf[z][2]))
                    z += 1                
                y += 1
            sql = "INSERT IGNORE INTO %s (conteudoId,variancia) VALUES (%d,'%.18f');" % (TabelaTrendsTitulosVariancia,resultados[x][0],variancia(relevancia))
            #print(sql)
            cursor.execute(sql)
            x += 1   
    except ValueError as e:
        print(e)
        db.close()
    db.commit()
    db.close()
    return 0

def dicionario():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, termo VARCHAR(64) UNIQUE, status INT(9), PRIMARY KEY(id));" % (TabelaTermosPorClassesGramaticais,TabelaTermosPorClassesGramaticais)
    cursor.execute(sql)

    filename = "dic.txt"
    file = open(filename, encoding="utf8")
    for line in file:
        if "/" in line:
            if "B" in line.split('/')[1]:
                try:
                    #substantivo
                    cursor.execute("INSERT IGNORE INTO %s (termo,status) VALUES ('%s',1);" % (TabelaTermosPorClassesGramaticais,line.split('/')[0]))
                except:
                    pass
            if "È" in line.split('/')[1] or "L" in line.split('/')[1] or "M" in line.split('/')[1] or "j" in line.split('/')[1]:
                try:
                    #verbo
                    cursor.execute("INSERT IGNORE INTO %s (termo,status) VALUES ('%s',2);" % (TabelaTermosPorClassesGramaticais,line.split('/')[0]))
                except:
                    pass
            if "D" in line.split('/')[1]:
                try:
                    #adjetivo
                    cursor.execute("INSERT IGNORE INTO %s (termo,status) VALUES ('%s',3);" % (TabelaTermosPorClassesGramaticais,line.split('/')[0]))
                except:
                    pass
            if "Ì" in line.split('/')[1] or "Ï" in line.split('/')[1] or "I" in line.split('/')[1]:
                try:
                    #adverbio
                    cursor.execute("INSERT IGNORE INTO %s (termo,status) VALUES ('%s',4);" % (TabelaTermosPorClassesGramaticais,line.split('/')[0]))
                except:
                    pass
    db.commit()
    db.close()
    return 0


def gramaticalAnalysis():
    #analise morfológica
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, conteudoId INT(9) UNIQUE, gramatica VARCHAR(128), PRIMARY KEY(id));" % (TabelagramaticalAnalysisTitulo,TabelagramaticalAnalysisTitulo)
        print(cursor.execute(sql))

        sql = "SELECT a.id, a.titulo, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        topTitulos = cursor.fetchall()
        qtd_topTitulos = len(topTitulos)

        x = 0
        while x < qtd_topTitulos:
            gramatica = []
            frase = topTitulos[x][1].split(' ')
            for palavra in frase:
                #palavra = palavra.replace(" ","")
                word = palavra.replace(',','')
                word = palavra.replace('.','')
                word = palavra.replace('!','')
                word = palavra.replace('?','')
                word = palavra.replace('\n','')
                
                sql = "SELECT termo, status FROM %s WHERE termo='%s';" % (TabelaTermosPorClassesGramaticais,word)
                cursor.execute(sql)
                resultado = cursor.fetchall()
                if len(resultado) > 0:
                    gramatica.append(resultado[0][1])
                else:
                    gramatica.append(0)
            #sql = "INSERT IGNORE INTO %s (conteudoId,gramatica) VALUES (%d,'%s');" % (TabelagramaticalAnalysisTitulo,topTitulos[x][0],"".join(str(i) for i in gramatica))
            sql = "INSERT IGNORE INTO %s (conteudoId,gramatica) VALUES (%d,'%s');" % (TabelagramaticalAnalysisTitulo,topTitulos[x][0],str(gramatica))
            cursor.execute(sql)
            x += 1
    except ValueError as e:
        print(e)
    db.commit()
    db.close()
    return 0

def gerarTabelaIndiceTags():
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, tag VARCHAR(32) UNIQUE, PRIMARY KEY(id));" % (TabelaIndiceTags,TabelaIndiceTags)
        print(cursor.execute(sql))

        sql = "SELECT tags FROM %s;" % (TabelaIndiceConteudo)
        cursor.execute(sql)
        tagsSet = cursor.fetchall()
        qtd_tags = len(tagsSet)
        #print(qtd_tags)

        #tags_por_artigo = []
        x = 0
        while x < qtd_tags:
            tags_por_artigo = tagsSet[x][0]
            tags_por_artigo = tags_por_artigo.replace("\"","")
            tags_por_artigo = tags_por_artigo.replace("[","")
            tags_por_artigo = tags_por_artigo.replace("]","")
            tags_por_artigo = tags_por_artigo.split(",")
            for tags in tags_por_artigo:
                sql = "INSERT IGNORE INTO %s (tag) VALUES ('%s');" % (TabelaIndiceTags,tags)
                cursor.execute(sql)
            x += 1
    except:
        pass
    db.commit()
    db.close()
    return 0


def gerarTabelaTagsFrequencia():
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, tag VARCHAR(64) UNIQUE, contador INT(12) DEFAULT 1, PRIMARY KEY(id));" % (TabelaTagsFrequencia,TabelaTagsFrequencia)
        print(cursor.execute(sql))

        sql = "SELECT tags FROM %s;" % (SourceTabela)
        cursor.execute(sql)
        tagsSet = cursor.fetchall()
        qtd_tags = len(tagsSet)
        #print(qtd_tags)
        #tags_por_artigo = []
        x = 0
        while x < qtd_tags:
            tags_por_artigo = tagsSet[x][0]
            tags_por_artigo = tags_por_artigo.replace("\"","")
            tags_por_artigo = tags_por_artigo.replace("[","")
            tags_por_artigo = tags_por_artigo.replace("]","")
            tags_por_artigo = tags_por_artigo.split(",")
            for tags in tags_por_artigo:
                sql = "INSERT INTO %s (tag) VALUES ('%s') ON DUPLICATE KEY UPDATE contador=contador+1;" % (TabelaTagsFrequencia,tags)
                cursor.execute(sql)
            x += 1
    except ValueError as e:
        print(e)
    db.commit()
    db.close()
    return 0

def gerarTabelaTagsTrendsFrequencia():
    try:
        db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
        cursor = db.cursor()

        sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, tag VARCHAR(128) UNIQUE, contador INT(12) DEFAULT 1, PRIMARY KEY(id));" % (TabelaTagsTrendsFrequencia,TabelaTagsTrendsFrequencia)
        print(cursor.execute(sql))

        sql = "SELECT a.tags, b.mediarating FROM %s a JOIN (SELECT a.content_id, a.acessos, a.mediarating, b.grupoId, b.views FROM %s a JOIN (SELECT a.conteudoId, b.grupoId, b.views FROM %s a JOIN (SELECT a.grupoId, SUM(b.acessos) as 'views', b.acessos, b.mediarating FROM %s a JOIN contentStats b ON b.content_id = a.conteudoId GROUP BY a.grupoId ORDER BY views DESC LIMIT %d) b ON a.grupoId = b.grupoId) b ON a.content_id = b.conteudoId ORDER BY a.acessos DESC) b ON a.id = b.content_id HAVING b.mediarating > 1 ORDER BY acessos DESC;" % (TabelaIndiceConteudo,TabelaConteudoStats,TabelaGruposTags,TabelaGruposTags,trendsLimit)
        cursor.execute(sql)
        tagsSet = cursor.fetchall()
        qtd_tags = len(tagsSet)
        #print(qtd_tags)
        #tags_por_artigo = []
        x = 0
        while x < qtd_tags:
            tags_por_artigo = tagsSet[x][0]
            tags_por_artigo = tags_por_artigo.replace("\"","")
            tags_por_artigo = tags_por_artigo.replace("[","")
            tags_por_artigo = tags_por_artigo.replace("]","")
            tags_por_artigo = tags_por_artigo.split(",")
            for tags in tags_por_artigo:
                sql = "INSERT INTO %s (tag) VALUES ('%s') ON DUPLICATE KEY UPDATE contador=contador+1;" % (TabelaTagsTrendsFrequencia,tags)
                cursor.execute(sql)
            x += 1
    except ValueError as e:
        print(e)
    db.commit()
    db.close()
    return 0


def GrupoConsumoPorGrupoConsumidor():
    # consulta grupos de conteúdo mais consumidor por grupos de usuários
    # select count(id) as 'populacao', grupoConsumidorId as 'grupo consumidores' , grupoConteudoId as 'grupo conteudos' from consumo group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;
    
    # alcance de um determonado grupo de conteudo
    # select count(id) as 'populacao', grupoConsumidorId, grupoConteudoId from consumo where grupoConteudoId = 3 group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;

    # consulta artigos mais acessador por grupo de conteúdo consumidos pelos grupos de usuários mais povoados
    # select a.acessos, a.content_id as artigo, b.grupoId as 'grupo conteudo'from contentstats a join (select a.conteudoId,a.grupoId from grupostags a join (select distinct a.grupoconteudoId from consumo a) b on a.grupoId = b.grupoconteudoId) b on a.content_id = b.conteudoid having a.acessos > 100 order by a.acessos desc;
    # select b.acessos, a.id, a.url, b.grupoId from indiceptt a join (select a.acessos, a.content_id, b.grupoId from contentstats a join (select a.conteudoId,a.grupoId from grupostags a join (select distinct a.grupoconteudoId from consumo a) b on a.grupoId = b.grupoconteudoId) b on a.content_id = b.conteudoid having a.acessos > 100) b on a.id = b.content_id order by b.acessos desc;
    
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoConsumidorId INT(9) NOT NULL, grupoConteudoId INT(9), PRIMARY KEY(id));" % (TabelaConsumo,TabelaConsumo)
    print(cursor.execute(sql))

    # TOP5 grupos consumidores mais densamente povoados
    #sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId ORDER BY total DESC LIMIT %d;" % (trendsLimit)
    #sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId ORDER BY total DESC LIMIT 50;"
    sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId ORDER BY total DESC;"
    # exclui o grupo 16 com a clausula having
    #sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId HAVING grupoId <> 16 ORDER BY total DESC LIMIT 15;"
    cursor.execute(sql)
    gruposConsumidores = cursor.fetchall()
    qtd_gruposConsumidores = len(gruposConsumidores)
    i = 0
    while i < qtd_gruposConsumidores:
        grupoId = gruposConsumidores[i][0]
        sql = "select a.grupos, b.grupoId from %s a join (select a.consumidorId, a.grupoId from %s a where a.grupoId = %d) b on a.consumidorId = b.consumidorId;" % (TabelaAgrupamento,TabelaGruposConsumidores,grupoId)
        cursor.execute(sql)
        consumo = cursor.fetchall()
        qtd_consumo = len(consumo)
        j = 0
        while j < qtd_consumo:
            grupoConsumo = consumo[j][0].split(",")
            grupoConsumidor = consumo[j][1]
            for grupoConteudo in grupoConsumo:
                grupoConteudo = grupoConteudo.replace("[","")
                grupoConteudo = grupoConteudo.replace("]","")            
                try:
                    sql = "INSERT IGNORE INTO %s (grupoConsumidorId,grupoConteudoId) VALUES (%d,%d);" % (TabelaConsumo,grupoConsumidor,int(grupoConteudo))
                    cursor.execute(sql)
                except:
                    pass
            j += 1
        i += 1
    db.close()
    return 0

def GrupoConsumoTitulosPorGrupoConsumidor():
    # consulta grupos de conteúdo mais consumidor por grupos de usuários
    # select count(id) as 'populacao', grupoConsumidorId as 'grupo consumidores' , grupoConteudoId as 'grupo conteudos' from consumo group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;
    
    # alcance de um determonado grupo de conteudo
    # select count(id) as 'populacao', grupoConsumidorId, grupoConteudoId from consumo where grupoConteudoId = 3 group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;

    # consulta artigos mais acessador por grupo de conteúdo consumidos pelos grupos de usuários mais povoados
    # select a.acessos, a.content_id as artigo, b.grupoId as 'grupo conteudo'from contentstats a join (select a.conteudoId,a.grupoId from grupostags a join (select distinct a.grupoconteudoId from consumo a) b on a.grupoId = b.grupoconteudoId) b on a.content_id = b.conteudoid having a.acessos > 100 order by a.acessos desc;
    # select b.acessos, a.id, a.url, b.grupoId from indiceptt a join (select a.acessos, a.content_id, b.grupoId from contentstats a join (select a.conteudoId,a.grupoId from grupostags a join (select distinct a.grupoconteudoId from consumo a) b on a.grupoId = b.grupoconteudoId) b on a.content_id = b.conteudoid having a.acessos > 100) b on a.id = b.content_id order by b.acessos desc;
    
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT(9) AUTO_INCREMENT, grupoConsumidorId INT(9) NOT NULL, grupoConteudoId INT(9), PRIMARY KEY(id));" % (TabelaConsumoTitulos,TabelaConsumoTitulos)
    print(cursor.execute(sql))

    # TOP5 grupos consumidores mais densamente povoados
    #sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId ORDER BY total DESC LIMIT %d;" % (trendsLimit)
    #sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId ORDER BY total DESC LIMIT 50;"
    sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId ORDER BY total DESC;"
    # exclui o grupo 16 com a clausula having
    #sql = "SELECT grupoId, COUNT(id) AS total FROM gruposconsumidores GROUP BY grupoId HAVING grupoId <> 16 ORDER BY total DESC LIMIT 15;"
    cursor.execute(sql)
    gruposConsumidores = cursor.fetchall()
    qtd_gruposConsumidores = len(gruposConsumidores)
    i = 0
    while i < qtd_gruposConsumidores:
        grupoId = gruposConsumidores[i][0]
        sql = "select a.grupos, b.grupoId from %s a join (select a.consumidorId, a.grupoId from %s a where a.grupoId = %d) b on a.consumidorId = b.consumidorId;" % (TabelaAgrupamentoTitulos,TabelaGruposConsumidores,grupoId)
        cursor.execute(sql)
        consumo = cursor.fetchall()
        qtd_consumo = len(consumo)
        j = 0
        while j < qtd_consumo:
            grupoConsumo = consumo[j][0].split(",")
            grupoConsumidor = consumo[j][1]
            for grupoConteudo in grupoConsumo:
                grupoConteudo = grupoConteudo.replace("[","")
                grupoConteudo = grupoConteudo.replace("]","")            
                try:
                    sql = "INSERT IGNORE INTO %s (grupoConsumidorId,grupoConteudoId) VALUES (%d,%d);" % (TabelaConsumoTitulos,grupoConsumidor,int(grupoConteudo))
                    cursor.execute(sql)
                except:
                    pass
            j += 1
        i += 1
    db.close()
    return 0

def consultaClasseGramatical(termo):
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "SELECT status FROM %s WHERE termo = '%s';" % (TabelaTermosPorClassesGramaticais,termo)
    cursor.execute(sql)

    resultado = cursor.fetchall()

    db.close()

    return resultado[0][0]

def stopword(word):
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "SELECT status FROM %s WHERE termo = '%s';" % (TabelaTermosPorClassesGramaticais,word)
    
    cursor.execute(sql)
    resultado = cursor.fetchall()
    qtd_resultado = len(resultado)
    #print(sql,qtd_resultado)

    if qtd_resultado > 0:
        #print(resultado[0][0])
        if resultado[0][0] == 1:
            return 0
        if resultado[0][0] == 2:
            return 0
        if resultado[0][0] == 3:
            return 0
        if resultado[0][0] == 4:
            return 0
    db.close()
    return 1

def sugere(dataSet):
    
    # estrutura gramatical dos trends:
    #  select a.gramatica, b.acessos, b.content_id from gramaticaanalysistitulos a join (select content_id, acessos from contentStats order by acessos desc limit 50) b on b.content_id = a.conteudoId;
    
    sugestao = []
    frase = dataSet.split(" ")
    qtd_palavras = len(frase)
   
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    if qtd_palavras > 0:
        ultima_palavra = frase[qtd_palavras-1]
    else:
        return ""

    # pesquisa ultima_palavra entre os titulos mais populares
    #select a.titulo from indiceptt a join (select * from contentstats order by acessos desc limit 50) b on b.content_id = a.id;
    #SELECT a.id, CASE WHEN a.titulo LIKE '%amazonino%' THEN SUBSTRING(a.titulo,LOCATE('amazonino',a.titulo)+LENGTH('amazonino'),LENGTH(a.titulo)) END as text FROM indiceptt a HAVING text IS NOT NULL;
    sql = "SELECT a.id, CASE WHEN a.titulo LIKE %s THEN SUBSTRING(a.titulo,LOCATE('%s',a.titulo)+LENGTH('%s'),LENGTH(a.titulo)) END as text FROM %s a HAVING text IS NOT NULL;" % ("\'%"+ultima_palavra+"%\'",ultima_palavra,ultima_palavra,TabelaIndiceConteudo)
    cursor.execute(sql)
    titulosTrends = cursor.fetchall()
    qtd_titulosTrends = len(titulosTrends)
    termosCandidatos = []
    x = 0
    while x < qtd_titulosTrends:
        titulo = titulosTrends[x][1].split(" ")

        if "" in titulo:
            titulo.remove("")
        if "," in titulo:
            titulo.remove(",")
        if "." in titulo:
            titulo.remove(".")

        if len(titulo) > 0:
            termosCandidatos.append(titulo[0])
        x += 1
    
    qtd_termosCandidatos = len(termosCandidatos)
    probabilidadeCondicional = 0.0
    #print("termosCandidatos: ", termosCandidatos)
    x = 0
    relevancia = 0.0
    relev = []
    while x < qtd_termosCandidatos:
        relevanciatmp = 0.0
        candidato = termosCandidatos[x]
        sql = "SELECT COUNT(*) FROM %s WHERE titulo LIKE %s;" % (TabelaIndiceConteudo,"\'%"+ultima_palavra+" % "+candidato+" %\'")
        cursor.execute(sql)
        resultado = cursor.fetchall()
    
        sql = "select (a.idf*a.tf) as relevancia from %s a where a.termoid = (select id from termos where termo = '%s');" % (TabelaTermosTFIDFTitulo,candidato)
        cursor.execute(sql)
        tfidf = cursor.fetchall()
        qtd_tfidf = len(tfidf)
        
        probabilidadeConjunta = resultado[0][0]
        bayes = probabilidadeConjunta/qtd_titulosTrends
        #print(probabilidadeConjunta,qtd_titulosTrends)
        if probabilidadeCondicional < bayes:
            #tf.idf de não stopwords
            if stopword(candidato) == 0:
                if qtd_tfidf > 0:
                    relevanciatmp = tfidf[0][0]
                if relevanciatmp > relevancia:
                    relevancia = relevanciatmp
                    probabilidadeCondicional = bayes
        if candidato in sugestao:
            pass
        else:
            sugestao.append(candidato)
            relev.append(bayes)
            #print(candidato,stop)
        x += 1
    db.close()
    print(relev)
    return sugestao

def alcancePorGrupoTags(grupoTags):
    alcance = 0
    # alcance de um determonado grupo de conteudo
    # select count(id) as 'populacao', grupoConsumidorId, grupoConteudoId from consumo where grupoConteudoId = 3 group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    sql = "select count(id) as 'populacao', grupoConsumidorId, grupoConteudoId from consumo where grupoConteudoId = %d group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;" % (grupoTags)
    cursor.execute(sql)
    resultado = cursor.fetchall()
    if len(resultado) > 0:
        alcance = resultado[0][0]
    db.close()

    return alcance

def alcancePorGrupoTitulo(grupoTitulo):
    alcance = 0
    # alcance de um determonado grupo de conteudo
    # select count(id) as 'populacao', grupoConsumidorId, grupoConteudoId from consumo where grupoConteudoId = 3 group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()
    
    sql = "select count(id) as 'populacao', grupoConsumidorId, grupoConteudoId from consumoTitulos where grupoConteudoId = %d group by grupoconsumidorId,grupoconteudoId order by 'acessos pelo grupo consumidor' desc;" % (grupoTitulo)
    print(sql)
    cursor.execute(sql)
    resultado = cursor.fetchall()
    if len(resultado) > 0:
        alcance = resultado[0][0]
    db.close()

    return alcance

def grupoPorTags(tags):
    #selecionar todas as tags medias e encontrar a maior similaridade jaccard
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "select a.tags,b.grupoId from %s a join (select grupoId,conteudoId from %s) b on a.id = b.conteudoId;" % (TabelaIndiceConteudo,TabelaGruposTagsMedios)
    cursor.execute(sql)
    resultado = cursor.fetchall()
    qtd_resultado = len(resultado)
    
    similaridade = 0.0
    grupo = 0

    x = 0
    while x < qtd_resultado:
        similaridadetmp = jaccardSimilarity_lista(stemmingTags(tags),stemmingTags(resultado[x][0]))
        if similaridadetmp > similaridade:
            similaridade = similaridadetmp
            grupo = resultado[x][1]
        x += 1
    
    db.close()
    return grupo

def grupoPorTitulo(titulo):
    #selecionar todos os titulos medios e encontra a maior similaridade jaccard
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "select a.titulo,b.grupoId from %s a join (select grupoId,conteudoId from %s) b on a.id = b.conteudoId;" % (TabelaIndiceConteudo,TabelaGruposTitulos)
    print(sql)
    cursor.execute(sql)
    resultado = cursor.fetchall()
    qtd_resultado = len(resultado)
    
    similaridade = 0.0
    grupo = 0

    x = 0
    while x < qtd_resultado:
        similaridadetmp = jaccard_similarity_titulo(stemmingTitle(titulo),stemmingTitle(resultado[x][0]))
        print(similaridadetmp," :: ",stemmingTitle(titulo)," :: ",stemmingTitle(resultado[x][0]))
        if similaridadetmp > similaridade:
            similaridade = similaridadetmp
            grupo = resultado[x][1]
        x += 1
    
    db.close()
    print(grupo)
    return grupo

def alcancePorTags(tags):
    alcance = alcancePorGrupoTags(grupoPorTags(tags))
    return alcance

def alcancePorTitulo(titulo):
    alcance = alcancePorGrupoTitulo(grupoPorTitulo(titulo))
    return alcance
    
#print(consultaClasseGramatical("negou"))

#print("indexarConteudo: ",indexarConteudo())                                                                                       # gerar indice contento urls, titulos e tags
#print("filtragemColaborativa: ",filtragemColaborativa())                                                                           # calcula o tempo gasto por página e o converte para rating log7(x). Gera uma Tabela contendo a Session_id, a Page_id e o Rating
print("gerarTabelaSimilaridadeTitulos: ",gerarTabelaSimilaridadeTitulos())                                                         # similaridade titulos. Gera uma Tabela contendo os Page_id e a similaridade de Jaccard para titulos
print("gerarTabelaSimilaridadeTags: ",gerarTabelaSimilaridadeTags())                                                               # similaridade tags. Gera uma Tabela contendo os Page_id e a similaridade de Jaccard para tags
#print("ratingsPorConsumidor: ",ratingsPorConsumidor())                                                                             # cadeia de ratings das paginas por usuário
print("contentStats: ",conteudoStats())                                                                                            # popularidade do conteúdo - calcular a "proporção da popularidade" com base nas datas de publicação! pegar também as datas de publicação dos conteúdos!
                                                                                                                                    # SELECT a.content_id AS pagina, a.acessos AS 'numero de acessos', a.mediarating AS 'rating medio', b.url FROM contentstats a JOIN indiceptt b ON b.id = a.content_id ORDER BY a.acessos DESC LIMIT 100;
print("agruparConteudosPorTags(): ",agruparConteudosPorTags())
print("agruparConteudosPorTitulos(): ",agruparConteudosPorTitulos())
print(gruposConsumidores())                                                                                                        # select count(distinct id) as 'total', grupoId from gruposconsumidores group by grupoId order by total;
print("gerarTabelaSimilaridadeConsumidoresPorGruposDeConteudosConsumidos(): ",gerarTabelaSimilaridadeConsumidoresPorGruposDeConteudosConsumidos())
print("gerarTabelaConsumidoresMediosPorGrupo(): ", gerarTabelaConsumidoresMediosPorGrupo())
print("gerarTabelaConteudosMediosPorGrupo(): ", gerarTabelaConteudosMediosPorGrupo())
print("gerarTabelaTagsMediosPorGrupo(): ", gerarTabelaTagsMediosPorGrupo())
print("gerarTabelaTitulosMediosPorGrupo(): ", gerarTabelaTitulosMediosPorGrupo())
print("gerarTabelaSimilaridadeGruposConsumidores(): ", gerarTabelaSimilaridadeGruposConsumidores())
print("gerarT'labelaSimilaridadeGruposTags(): ", gerarTabelaSimilaridadeGruposTags())
print("gerarTabelaDeGruposDeConteudosConsumidosPorConsumidor: ",gerarTabelaDeGruposDeConteudosConsumidosPorConsumidor())           # SELECT a.session_id, a.content_id, a.rating, b.grupoId FROM filtragemcolaborativa a JOIN grupostags b ON b.conteudoId = a.content_id WHERE a.session_id = 5 GROUP BY a.content_id ORDER BY a.rating;
print("gerarTabelaDeGruposDeConteudosPorTituloConsumidosPorConsumidor: ",gerarTabelaDeGruposDeConteudosPorTituloConsumidosPorConsumidor())
print("gerarTabelaSimilaridadeGruposTitulos(): ", gerarTabelaSimilaridadeGruposTitulos())
#print("representanteGrupoConsumidores(5): ",representanteGrupoConsumidorepresentanteGrupoConsumidores(5))
print(gerarTabelaIndiceTermos())

#print(recomendacaoBayes('["amazonas","amazonino mendes","Arthur","Arthur neto","cu00e9u nublado","eleiu00e7u00f5es","governador","prefeito","Prefeito de Manaus"]'))
 
#print("trendsTermosTFIDFTitulo(): ",trendsTermosTFIDFTitulo())
#print("termosTFIDFTitulo(): ",termosTFIDFTitulo())
#print("termosTFIDFConteudo(): ",termosTFIDFConteudo())
#print("analysisTitulos(): ",analysisTitulos())
#print("topTitulosIDFmedio(): ",topTitulosIDFmedio())

#print("tituloTrendsVariancia(): ",tituloTrendsVariancia())
#print(gerarTabelaTagsTrendsFrequencia())
#print(gerarTabelaTermosTitulosTrendsFrequencia())
#print(gerarTabelaTermosConteudosTrendsFrequencia())
#dicionario()
#gramaticalAnalysis()
#print(sugere("prefeito"))
#print(alcancePorGrupoTags(3))
#print(alcancePorTags("[\"sindicato\",\"reajuste\",\"professores\"]"))
#print(alcancePorTitulo("Amazonino, Arthur e Lula sao pecas chaves no jogo"))
#Amazonino, Arthur e Lula sao pecas chaves no jogo eleitoral de 2018
#GrupoConsumoPorGrupoConsumidor()
#GrupoConsumoTitulosPorGrupoConsumidor()
#print(removestopwords("Lula da silva do bnc testar"))
# Tabela RATING - TEMPO
#   0   <   7
#   1   ~   7
#   2   ~   15
#   3   ~   30
#   4   ~   60
#   5   >   60
#///print(gerarTabelaStopWords())
"""
def termosupdate():
    db = MySQLdb.connect(BancoLocal,BancoUsuario,BancoSenha,Banco)
    cursor = db.cursor()

    sql = "SELECT termo,status FROM stopwords;"
    cursor.execute(sql)
    novosTermos = cursor.fetchall()
    qtd_novosTermos = len(novosTermos)

    x = 0
    while x < qtd_novosTermos:
        try:
            sql = "INSERT INTO termos (termo,status) VALUES ('%s',%d);" % (novosTermos[x][0],novosTermos[x][1])
            cursor.execute(sql)
        except:
            sql = "UPDATE termos SET status = %d WHERE termo = '%s';" % (novosTermos[x][1],novosTermos[x][0])
            cursor.execute(sql)
        x +=1
    db.close()
    return 0
print(termosupdate())
"""
