from Crypto.Cipher import AES
from binascii import a2b_hex


def add_to_16(text):
    if len(text.encode('utf-8')) % 16:
        add = 16 - (len(text.encode('utf-8')) % 16)
    else:
        add = 0
    text = text + ('\0' * add)
    return text.encode('utf-8')


# AES解密
def decrypts(encrData):
    iv = b"0123456789ABCDEF"
    key = "jo8j9wGw%6HbxfFn".encode('utf-8')
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plain = cipher.decrypt(a2b_hex(encrData))
    return bytes.decode(plain).rsplit('\0')


if __name__ == '__main__':
    data = b'95780ba0943730051dccb5fe3918f9fe4c6612ab8a332ee7d1067088471faa625bb393a2cdbace8b44a018deafa2979da85669a8018af83b268881d99740f520fa35b4291dc9fbb6f897b8ddbffd83cfc8956b6a48a2ee913e188a1daff805a63937b392190a87fe65d2231465b49e00a5d89a3ef1b93dd78184a3596a2a0e3d3c6d9768d8330432fa9ecc0bb5714965c304a6721c710a4967d62d023ec2d97e67479391e68665cf7678a45464944a5a409a22b7adcf65babda2cf9d38b9891203fd1e16f36d3790a6d1f8133440537bc448ea8e9e355bf1a5a9363b18887e4705df014ab4ee0cd87b589000742687253ec8b3f8afec619059efabff3f07b786f06ef7ab1cb08bf25fc3d55d4be2b6a3bc5b5c863d63b2659e819e13d4e8e0178b5eda724977be6033663abff2fcd0a34771ab81760f1754e67ba45687ae451bbb552e02cc2e25668576ac6b4c890a886582751d8546879268754e3eceeefba724592ed222a5ab075a2576fd7870ec98311cee95df55a0689b1eb10b872689f8c7207c421a27fad4e9b9a58073d3cf4b82c6e4fe2fec32a734031fc0e1d823361781f178e6a8db8d638e9271a7c08237ffb6da73a030415000afdb427bb89b10ab3f939495c81126f1cb56641655078f4c727b1b4268827c7710288bd74394ea923c0e875c9b80560564ff9069c9a56cb49a80007382320bff8bba71a8c12afbcd67f73ed25a55ea87078ef2e51769c78444ea1002e6754efcd666bcfc38074e65c304ca81cb3e802cce99005caade168a1d285dc520ae9d7b4bccdf3af25e02'

    res = decrypts(data)

    print(res)
