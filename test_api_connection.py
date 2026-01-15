#!/usr/bin/env python3
"""
Script de teste de conectividade com APIs
"""
import requests
import sys
import socket

def test_dns():
    """Testa resolução DNS"""
    print("🔍 Testando DNS...")
    try:
        ip = socket.gethostbyname('viacep.com.br')
        print(f"✅ DNS OK - viacep.com.br resolve para: {ip}")
        return True
    except socket.gaierror as e:
        print(f"❌ DNS falhou: {e}")
        return False

def test_viacep():
    """Testa API ViaCEP"""
    print("\n🔍 Testando ViaCEP API...")
    try:
        url = "https://viacep.com.br/ws/01310100/json/"
        print(f"   URL: {url}")
        
        response = requests.get(url, timeout=10)
        print(f"✅ Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get('erro'):
                print(f"❌ CEP não encontrado")
                return False
            else:
                print(f"✅ CEP encontrado:")
                print(f"   Logradouro: {data.get('logradouro')}")
                print(f"   Bairro: {data.get('bairro')}")
                print(f"   Cidade: {data.get('localidade')}/{data.get('uf')}")
                return True
        else:
            print(f"❌ Status inesperado: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erro de conexão: {type(e).__name__}")
        print(f"   Detalhes: {str(e)[:200]}")
        return False
    except requests.exceptions.Timeout:
        print(f"❌ Timeout - API não respondeu em 10 segundos")
        return False
    except Exception as e:
        print(f"❌ Erro inesperado: {type(e).__name__}: {e}")
        return False

def test_nominatim():
    """Testa API Nominatim"""
    print("\n🔍 Testando Nominatim API...")
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': 'Avenida Paulista, São Paulo, BR',
            'format': 'json',
            'limit': 1
        }
        headers = {'User-Agent': 'GeoGrafi/1.0'}
        
        print(f"   URL: {url}")
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"✅ Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"✅ Localização encontrada:")
                print(f"   Nome: {data[0].get('display_name')}")
                print(f"   Lat/Lon: {data[0].get('lat')}, {data[0].get('lon')}")
                return True
            else:
                print(f"❌ Nenhum resultado encontrado")
                return False
        else:
            print(f"❌ Status inesperado: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erro de conexão: {type(e).__name__}")
        return False
    except requests.exceptions.Timeout:
        print(f"❌ Timeout")
        return False
    except Exception as e:
        print(f"❌ Erro inesperado: {type(e).__name__}: {e}")
        return False

def main():
    print("=" * 60)
    print("🌐 TESTE DE CONECTIVIDADE COM APIs")
    print("=" * 60)
    
    results = []
    
    # Testa DNS
    results.append(("DNS", test_dns()))
    
    # Testa ViaCEP
    results.append(("ViaCEP", test_viacep()))
    
    # Testa Nominatim
    results.append(("Nominatim", test_nominatim()))
    
    # Resumo
    print("\n" + "=" * 60)
    print("📊 RESUMO DOS TESTES")
    print("=" * 60)
    
    for name, success in results:
        status = "✅ OK" if success else "❌ FALHOU"
        print(f"{name:20} {status}")
    
    print("=" * 60)
    
    # Retorna código de saída
    all_success = all(result[1] for result in results)
    if all_success:
        print("\n✨ Todas as APIs estão funcionando!")
        sys.exit(0)
    else:
        print("\n⚠️  Alguns serviços não estão acessíveis")
        print("   Isso é normal em ambientes de desenvolvimento containerizados")
        sys.exit(1)

if __name__ == "__main__":
    main()
