import os
import kmall

# 2019 Thunder Bay - With Water Column
#file = 'data/0019_20190511_204630_ASVBEN.kmall'

#2019 Pre-Samoa MAC Test Near Portsmouth
#file = '0006_20190529_161633_ASVBEN.kmall'

path = 'data'
for file in os.listdir(path):

    print('File: ', file)

    pathFile = path + '/' + file
    k = kmall.kmall(pathFile)
    k.OpenFiletoRead()
    k.index_file()

    #print(k.msgoffset, len(k.msgoffset))
    #print(k.msgsize, len(k.msgsize))
    #print(k.msgtype, len(k.msgtype))

    #pingCount = k.check_ping_count()
    #print(pingCount)

    # Get the file byte count offset for each IIP datagram.
    IIPOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#IIP'"]
    print("Num IIP Offsets: ", len(IIPOffsets))
    for offset in IIPOffsets:
        k.FID.seek(offset, 0)
        dg_IIP = k.read_EMdgmIIP()
        #print(dg_IIP)

    # Get the file byte count offset for each IOP datagram.
    IOPOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#IOP'"]
    print("Num IOP Offsets: ", len(IOPOffsets))
    for offset in IOPOffsets:
        k.FID.seek(offset, 0)
        dg_IOP = k.read_EMdgmIOP()
        #print(dg_IOP)

    # Get the file byte count offset for each IBE datagram.
    IBEOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#IBE'"]
    print("Num IBE Offsets: ", len(IBEOffsets))
    for offset in IBEOffsets:
        k.FID.seek(offset, 0)
        dg_IBE = k.read_EMdgmIB()
        #print(dg_IBE)

    # Get the file byte count offset for each IBR datagram.
    IBROffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#IBR'"]
    print("Num IBR Offsets: ", len(IBROffsets))
    for offset in IBROffsets:
        k.FID.seek(offset, 0)
        dg_IBR = k.read_EMdgmIB()
        #print(dg_IBR)

    # Get the file byte count offset for each IBS datagram.
    IBSOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#IBS'"]
    print("Num IBS Offsets: ", len(IBSOffsets))
    for offset in IBSOffsets:
        k.FID.seek(offset, 0)
        dg_IBS = k.read_EMdgmIB()
        # print(dg_IBS)

    # Get the file byte count offset for each MRZ datagram.
    MRZOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#MRZ'"]
    print("Num MRZ Offsets: ", len(MRZOffsets))
    for offset in MRZOffsets:
        k.FID.seek(offset, 0)
        dg_MRZ = k.read_EMdgmMRZ()
        #print(dg_MRZ)
    
    # Get the file byte count offset for each MWC datagram.
    MWCOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#MWC'"]
    print("Num MWC Offsets: ", len(MWCOffsets))
    for offset in MWCOffsets:
        k.FID.seek(offset, 0)
        dg_MWC = k.read_EMdgmMWC()
        #print(dg_MWC)
    
    # Get the file byte count offset for each SPO datagram.
    SPOOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SPO'"]
    print("Num SPO Offsets: ", len(SPOOffsets))
    for offset in SPOOffsets:
        k.FID.seek(offset, 0)
        dg_SPO = k.read_EMdgmSPO()
        #print(dg_SPO)

    # Get the file byte count offset for each SKM datagram.
    SKMOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SKM'"]
    print("Num SKM Offsets: ", len(SKMOffsets))
    for offset in SKMOffsets:
        k.FID.seek(offset, 0)
        dg_SKM = k.read_EMdgmSKM()
        #print(dg_SKM)

    # Get the file byte count offset for each SVP datagram.
    SVPOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SVP'"]
    print("Num SVP Offsets: ", len(SVPOffsets))
    for offset in SVPOffsets:
        k.FID.seek(offset, 0)
        dg_SVP = k.read_EMdgmSVP()
        #print(dg_SVP)

    # Get the file byte count offset for each SVT datagram.
    SVTOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SVT'"]
    print("Num SVT Offsets: ", len(SVTOffsets))
    for offset in SVTOffsets:
        k.FID.seek(offset, 0)
        dg_SVT = k.read_EMdgmSVT()
        #print(dg_SVT)

    # Get the file byte count offset for each SCL datagram.
    SCLOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SCL'"]
    print("Num SCL Offsets: ", len(SCLOffsets))
    for offset in SCLOffsets:
        k.FID.seek(offset, 0)
        dg_SCL = k.read_EMdgmSCL()
        #print(dg_SCL)

    # Get the file byte count offset for each SDE datagram.
    SDEOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SDE'"]
    print("Num SDE Offsets: ", len(SDEOffsets))
    for offset in SDEOffsets:
        k.FID.seek(offset, 0)
        dg_SDE = k.read_EMdgmSDE()
        #print(dg_SCL)

    # Get the file byte count offset for each SHI datagram.
    SHIOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#SHI'"]
    print("Num SHI Offsets: ", len(SHIOffsets))
    for offset in SDEOffsets:
        k.FID.seek(offset, 0)
        dg_SHI = k.read_EMdgmSHI()
        #print(dg_SHI)

    # Get the file byte count offset for each CPO datagram.
    CPOOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#CPO'"]
    print("Num CPO Offsets: ", len(CPOOffsets))
    for offset in CPOOffsets:
        k.FID.seek(offset, 0)
        dg_CPO = k.read_EMdgmCPO()
        #print(dg_CPO)

    # Get the file byte count offset for each CHE datagram.
    CHEOffsets = [x for x, y in zip(k.msgoffset, k.msgtype) if y == "b'#CHE'"]
    print("Num CHE Offsets: ", len(CHEOffsets))
    for offset in CHEOffsets:
        k.FID.seek(offset, 0)
        dg_CHE = k.read_EMdgmCHE()
        #print(dg_CHE)

    k.closeFile()