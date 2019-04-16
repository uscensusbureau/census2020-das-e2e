#!/usr/bin/env python3
# -*- coding: utf-8 -*-
def leftpad(x,width): return ' '*(width-len(str(x)))+str(x)
class MDF_Person_validator:
    @classmethod
    def is_valid_SCHEMA_TYPE_CODE(self,x):
        """Schema Type Code"""
        return True
    @classmethod
    def is_valid_SCHEMA_BUILD_ID(self,x):
        """Schema Build ID"""
        return True
    @classmethod
    def is_valid_TABBLKST(self,x):
        """2018 Tabulation State (FIPS)"""
        return (leftpad('1',2) <= leftpad(x,2) <= leftpad('2',2)) or (leftpad('4',2) <= leftpad(x,2) <= leftpad('6',2)) or (leftpad('8',2) <= leftpad(x,2) <= leftpad('13',2)) or (leftpad('15',2) <= leftpad(x,2) <= leftpad('42',2)) or (leftpad('44',2) <= leftpad(x,2) <= leftpad('51',2)) or (leftpad('53',2) <= leftpad(x,2) <= leftpad('56',2))
    @classmethod
    def is_valid_TABBLKCOU(self,x):
        """2018 Tabulation County (FIPS)"""
        return (leftpad('1',3) <= leftpad(x,3) <= leftpad('840',3))
    @classmethod
    def is_valid_TABTRACTCE(self,x):
        """2018 Tabulation Census Tract"""
        return (leftpad('100',6) <= leftpad(x,6) <= leftpad('998999',6))
    @classmethod
    def is_valid_TABBLKGRPCE(self,x):
        """2018 Census Block Group"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('9',1))
    @classmethod
    def is_valid_TABBLK(self,x):
        """2018 Block Number"""
        return (leftpad('1',4) <= leftpad(x,4) <= leftpad('9999',4))
    @classmethod
    def is_valid_EUID(self,x):
        """Privacy Edited Unit ID"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 999999999)
    @classmethod
    def is_valid_EPNUM(self,x):
        """Privacy Edited Person Number Note: For households, EPNUM = 1assigned to the householder(QREL = 01)"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 99999)
    @classmethod
    def is_valid_RTYPE(self,x):
        """Record Type"""
        return (leftpad(x,1)==leftpad('3',1)) or (leftpad(x,1)==leftpad('5',1))
    @classmethod
    def is_valid_QREL(self,x):
        """Relationship"""
        return (leftpad('1',2) <= leftpad(x,2) <= leftpad('19',2)) or (leftpad(x,2)==leftpad('99',2))
    @classmethod
    def is_valid_QSEX(self,x):
        """Sex"""
        return (leftpad('1',1) <= leftpad(x,1) <= leftpad('2',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_QAGE(self,x):
        """Age Note: For 2018 End-to-End QAGE = 17 assigned to minors, and QAGE = 18 assigned to voting age persons"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 115)
    @classmethod
    def is_valid_CENHISP(self,x):
        """Hispanic Origin"""
        return (leftpad('1',1) <= leftpad(x,1) <= leftpad('2',1))
    @classmethod
    def is_valid_CENRACE(self,x):
        """Census Race"""
        return (leftpad('1',2) <= leftpad(x,2) <= leftpad('63',2))
    @classmethod
    def is_valid_IMPRACE(self,x):
        """OMB Race"""
        return (leftpad(x,2)==leftpad('99',2))
    @classmethod
    def is_valid_QSPANX(self,x):
        """Hispanic Origin Group"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE1(self,x):
        """Detailed Race 1"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE2(self,x):
        """Detailed Race 2"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE3(self,x):
        """Detailed Race 3"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE4(self,x):
        """Detailed Race 4"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE5(self,x):
        """Detailed Race 5"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE6(self,x):
        """Detailed Race 6"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE7(self,x):
        """Detailed Race 7"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_QRACE8(self,x):
        """Detailed Race 8"""
        return (leftpad(x,4)==leftpad('9999',4))
    @classmethod
    def is_valid_CIT(self,x):
        """Citizenship"""
        return (leftpad(x,1)==leftpad('9',1))

    @classmethod
    def validate_pipe_delimited(self,x):
        fields = x.split('|')
        if len(fields)!=26: return False
        if is_valid_SCHEMA_TYPE_CODE(fields[1]) == False: return False
        if is_valid_SCHEMA_BUILD_ID(fields[2]) == False: return False
        if is_valid_TABBLKST(fields[3]) == False: return False
        if is_valid_TABBLKCOU(fields[4]) == False: return False
        if is_valid_TABTRACTCE(fields[5]) == False: return False
        if is_valid_TABBLKGRPCE(fields[6]) == False: return False
        if is_valid_TABBLK(fields[7]) == False: return False
        if is_valid_EUID(fields[8]) == False: return False
        if is_valid_EPNUM(fields[9]) == False: return False
        if is_valid_RTYPE(fields[10]) == False: return False
        if is_valid_QREL(fields[11]) == False: return False
        if is_valid_QSEX(fields[12]) == False: return False
        if is_valid_QAGE(fields[13]) == False: return False
        if is_valid_CENHISP(fields[14]) == False: return False
        if is_valid_CENRACE(fields[15]) == False: return False
        if is_valid_IMPRACE(fields[16]) == False: return False
        if is_valid_QSPANX(fields[17]) == False: return False
        if is_valid_QRACE1(fields[18]) == False: return False
        if is_valid_QRACE2(fields[19]) == False: return False
        if is_valid_QRACE3(fields[20]) == False: return False
        if is_valid_QRACE4(fields[21]) == False: return False
        if is_valid_QRACE5(fields[22]) == False: return False
        if is_valid_QRACE6(fields[23]) == False: return False
        if is_valid_QRACE7(fields[24]) == False: return False
        if is_valid_QRACE8(fields[25]) == False: return False
        if is_valid_CIT(fields[26]) == False: return False
        return True

class MDF_Person:
    __slots__ = ['SCHEMA_TYPE_CODE', 'SCHEMA_BUILD_ID', 'TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK', 'EUID', 'EPNUM', 'RTYPE', 'QREL', 'QSEX', 'QAGE', 'CENHISP', 'CENRACE', 'IMPRACE', 'QSPANX', 'QRACE1', 'QRACE2', 'QRACE3', 'QRACE4', 'QRACE5', 'QRACE6', 'QRACE7', 'QRACE8', 'CIT']
    def __repr__(self):
        return 'MDF_Person<SCHEMA_TYPE_CODE:{},SCHEMA_BUILD_ID:{},TABBLKST:{},TABBLKCOU:{},TABTRACTCE:{},TABBLKGRPCE:{},TABBLK:{},EUID:{},EPNUM:{},RTYPE:{},QREL:{},QSEX:{},QAGE:{},CENHISP:{},CENRACE:{},IMPRACE:{},QSPANX:{},QRACE1:{},QRACE2:{},QRACE3:{},QRACE4:{},QRACE5:{},QRACE6:{},QRACE7:{},QRACE8:{},CIT:{}>'.format(self.SCHEMA_TYPE_CODE,self.SCHEMA_BUILD_ID,self.TABBLKST,self.TABBLKCOU,self.TABTRACTCE,self.TABBLKGRPCE,self.TABBLK,self.EUID,self.EPNUM,self.RTYPE,self.QREL,self.QSEX,self.QAGE,self.CENHISP,self.CENRACE,self.IMPRACE,self.QSPANX,self.QRACE1,self.QRACE2,self.QRACE3,self.QRACE4,self.QRACE5,self.QRACE6,self.QRACE7,self.QRACE8,self.CIT)
    def __init__(self,line=None):
        if line: 
            if '|' in line: 
                self.parse_pipe(line)
            else:
                self.parse_fixed(line)
    def parse_pipe(self,line):
        cols = line.split('|')
        assert len(cols)==26
        self.SCHEMA_TYPE_CODE = cols[0]
        self.SCHEMA_BUILD_ID = cols[1]
        self.TABBLKST = cols[2]
        self.TABBLKCOU = cols[3]
        self.TABTRACTCE = cols[4]
        self.TABBLKGRPCE = cols[5]
        self.TABBLK = cols[6]
        self.EUID = cols[7]
        self.EPNUM = cols[8]
        self.RTYPE = cols[9]
        self.QREL = cols[10]
        self.QSEX = cols[11]
        self.QAGE = cols[12]
        self.CENHISP = cols[13]
        self.CENRACE = cols[14]
        self.IMPRACE = cols[15]
        self.QSPANX = cols[16]
        self.QRACE1 = cols[17]
        self.QRACE2 = cols[18]
        self.QRACE3 = cols[19]
        self.QRACE4 = cols[20]
        self.QRACE5 = cols[21]
        self.QRACE6 = cols[22]
        self.QRACE7 = cols[23]
        self.QRACE8 = cols[24]
        self.CIT = cols[25]

    def parse_fixed(self,line):
        self.SCHEMA_TYPE_CODE = None   # no position information for SCHEMA_TYPE_CODE
        self.SCHEMA_BUILD_ID = None   # no position information for SCHEMA_BUILD_ID
        self.TABBLKST = None   # no position information for TABBLKST
        self.TABBLKCOU = None   # no position information for TABBLKCOU
        self.TABTRACTCE = None   # no position information for TABTRACTCE
        self.TABBLKGRPCE = None   # no position information for TABBLKGRPCE
        self.TABBLK = None   # no position information for TABBLK
        self.EUID = None   # no position information for EUID
        self.EPNUM = None   # no position information for EPNUM
        self.RTYPE = None   # no position information for RTYPE
        self.QREL = None   # no position information for QREL
        self.QSEX = None   # no position information for QSEX
        self.QAGE = None   # no position information for QAGE
        self.CENHISP = None   # no position information for CENHISP
        self.CENRACE = None   # no position information for CENRACE
        self.IMPRACE = None   # no position information for IMPRACE
        self.QSPANX = None   # no position information for QSPANX
        self.QRACE1 = None   # no position information for QRACE1
        self.QRACE2 = None   # no position information for QRACE2
        self.QRACE3 = None   # no position information for QRACE3
        self.QRACE4 = None   # no position information for QRACE4
        self.QRACE5 = None   # no position information for QRACE5
        self.QRACE6 = None   # no position information for QRACE6
        self.QRACE7 = None   # no position information for QRACE7
        self.QRACE8 = None   # no position information for QRACE8
        self.CIT = None   # no position information for CIT
    def validate(self):
        """Return True if the object data validates"""
        if not MDF_Person_validator.is_valid_SCHEMA_TYPE_CODE(self.SCHEMA_TYPE_CODE): return False
        if not MDF_Person_validator.is_valid_SCHEMA_BUILD_ID(self.SCHEMA_BUILD_ID): return False
        if not MDF_Person_validator.is_valid_TABBLKST(self.TABBLKST): return False
        if not MDF_Person_validator.is_valid_TABBLKCOU(self.TABBLKCOU): return False
        if not MDF_Person_validator.is_valid_TABTRACTCE(self.TABTRACTCE): return False
        if not MDF_Person_validator.is_valid_TABBLKGRPCE(self.TABBLKGRPCE): return False
        if not MDF_Person_validator.is_valid_TABBLK(self.TABBLK): return False
        if not MDF_Person_validator.is_valid_EUID(self.EUID): return False
        if not MDF_Person_validator.is_valid_EPNUM(self.EPNUM): return False
        if not MDF_Person_validator.is_valid_RTYPE(self.RTYPE): return False
        if not MDF_Person_validator.is_valid_QREL(self.QREL): return False
        if not MDF_Person_validator.is_valid_QSEX(self.QSEX): return False
        if not MDF_Person_validator.is_valid_QAGE(self.QAGE): return False
        if not MDF_Person_validator.is_valid_CENHISP(self.CENHISP): return False
        if not MDF_Person_validator.is_valid_CENRACE(self.CENRACE): return False
        if not MDF_Person_validator.is_valid_IMPRACE(self.IMPRACE): return False
        if not MDF_Person_validator.is_valid_QSPANX(self.QSPANX): return False
        if not MDF_Person_validator.is_valid_QRACE1(self.QRACE1): return False
        if not MDF_Person_validator.is_valid_QRACE2(self.QRACE2): return False
        if not MDF_Person_validator.is_valid_QRACE3(self.QRACE3): return False
        if not MDF_Person_validator.is_valid_QRACE4(self.QRACE4): return False
        if not MDF_Person_validator.is_valid_QRACE5(self.QRACE5): return False
        if not MDF_Person_validator.is_valid_QRACE6(self.QRACE6): return False
        if not MDF_Person_validator.is_valid_QRACE7(self.QRACE7): return False
        if not MDF_Person_validator.is_valid_QRACE8(self.QRACE8): return False
        if not MDF_Person_validator.is_valid_CIT(self.CIT): return False
        return True
    def validate_reason(self):
        reason=[]
        if not MDF_Person_validator.is_valid_SCHEMA_TYPE_CODE(self.SCHEMA_TYPE_CODE): reason.append('SCHEMA_TYPE_CODE ('+str(self.SCHEMA_TYPE_CODE)+') out of range ()')
        if not MDF_Person_validator.is_valid_SCHEMA_BUILD_ID(self.SCHEMA_BUILD_ID): reason.append('SCHEMA_BUILD_ID ('+str(self.SCHEMA_BUILD_ID)+') out of range ()')
        if not MDF_Person_validator.is_valid_TABBLKST(self.TABBLKST): reason.append('TABBLKST ('+str(self.TABBLKST)+') out of range (1-2, 4-6, 8-13, 15-42, 44-51, 53-56)')
        if not MDF_Person_validator.is_valid_TABBLKCOU(self.TABBLKCOU): reason.append('TABBLKCOU ('+str(self.TABBLKCOU)+') out of range (1-840)')
        if not MDF_Person_validator.is_valid_TABTRACTCE(self.TABTRACTCE): reason.append('TABTRACTCE ('+str(self.TABTRACTCE)+') out of range (100-998999)')
        if not MDF_Person_validator.is_valid_TABBLKGRPCE(self.TABBLKGRPCE): reason.append('TABBLKGRPCE ('+str(self.TABBLKGRPCE)+') out of range (0-9)')
        if not MDF_Person_validator.is_valid_TABBLK(self.TABBLK): reason.append('TABBLK ('+str(self.TABBLK)+') out of range (1-9999)')
        if not MDF_Person_validator.is_valid_EUID(self.EUID): reason.append('EUID ('+str(self.EUID)+') out of range (0-999999999)')
        if not MDF_Person_validator.is_valid_EPNUM(self.EPNUM): reason.append('EPNUM ('+str(self.EPNUM)+') out of range (0-99999)')
        if not MDF_Person_validator.is_valid_RTYPE(self.RTYPE): reason.append('RTYPE ('+str(self.RTYPE)+') out of range (3-3, 5-5)')
        if not MDF_Person_validator.is_valid_QREL(self.QREL): reason.append('QREL ('+str(self.QREL)+') out of range (1-19, 99-99)')
        if not MDF_Person_validator.is_valid_QSEX(self.QSEX): reason.append('QSEX ('+str(self.QSEX)+') out of range (1-2, 9-9)')
        if not MDF_Person_validator.is_valid_QAGE(self.QAGE): reason.append('QAGE ('+str(self.QAGE)+') out of range (0-115)')
        if not MDF_Person_validator.is_valid_CENHISP(self.CENHISP): reason.append('CENHISP ('+str(self.CENHISP)+') out of range (1-2)')
        if not MDF_Person_validator.is_valid_CENRACE(self.CENRACE): reason.append('CENRACE ('+str(self.CENRACE)+') out of range (1-63)')
        if not MDF_Person_validator.is_valid_IMPRACE(self.IMPRACE): reason.append('IMPRACE ('+str(self.IMPRACE)+') out of range (99-99)')
        if not MDF_Person_validator.is_valid_QSPANX(self.QSPANX): reason.append('QSPANX ('+str(self.QSPANX)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE1(self.QRACE1): reason.append('QRACE1 ('+str(self.QRACE1)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE2(self.QRACE2): reason.append('QRACE2 ('+str(self.QRACE2)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE3(self.QRACE3): reason.append('QRACE3 ('+str(self.QRACE3)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE4(self.QRACE4): reason.append('QRACE4 ('+str(self.QRACE4)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE5(self.QRACE5): reason.append('QRACE5 ('+str(self.QRACE5)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE6(self.QRACE6): reason.append('QRACE6 ('+str(self.QRACE6)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE7(self.QRACE7): reason.append('QRACE7 ('+str(self.QRACE7)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_QRACE8(self.QRACE8): reason.append('QRACE8 ('+str(self.QRACE8)+') out of range (9999-9999)')
        if not MDF_Person_validator.is_valid_CIT(self.CIT): reason.append('CIT ('+str(self.CIT)+') out of range (9-9)')
        return ', '.join(reason)


def leftpad(x,width): return ' '*(width-len(str(x)))+str(x)
class MDF_Unit_validator:
    @classmethod
    def is_valid_SCHEMA_TYPE_CODE(self,x):
        """Schema Type Code"""
        return True
    @classmethod
    def is_valid_SCHEMA_BUILD_ID(self,x):
        """Schema Build ID"""
        return True
    @classmethod
    def is_valid_TABBLKST(self,x):
        """2018 Tabulation State (FIPS)"""
        return (leftpad('1',2) <= leftpad(x,2) <= leftpad('2',2)) or (leftpad('4',2) <= leftpad(x,2) <= leftpad('6',2)) or (leftpad('8',2) <= leftpad(x,2) <= leftpad('13',2)) or (leftpad('15',2) <= leftpad(x,2) <= leftpad('42',2)) or (leftpad('44',2) <= leftpad(x,2) <= leftpad('51',2)) or (leftpad('53',2) <= leftpad(x,2) <= leftpad('56',2))
    @classmethod
    def is_valid_TABBLKCOU(self,x):
        """2018 Tabulation County (FIPS)"""
        return (leftpad('1',3) <= leftpad(x,3) <= leftpad('840',3))
    @classmethod
    def is_valid_TABTRACTCE(self,x):
        """2018 Tabulation Census Tract"""
        return (leftpad('100',6) <= leftpad(x,6) <= leftpad('998999',6))
    @classmethod
    def is_valid_TABBLKGRPCE(self,x):
        """2018 Census Block Group"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('9',1))
    @classmethod
    def is_valid_TABBLK(self,x):
        """2018 Block Number"""
        return (leftpad('1',4) <= leftpad(x,4) <= leftpad('9999',4))
    @classmethod
    def is_valid_EUID(self,x):
        """Privacy Edited Unit ID"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 999999999)
    @classmethod
    def is_valid_RTYPE(self,x):
        """Record Type"""
        return (leftpad(x,1)==leftpad('2',1)) or (leftpad(x,1)==leftpad('4',1))
    @classmethod
    def is_valid_GQTYPE(self,x):
        """Group Quarters Type Note: For 2018 End-to_End, values will be assigned as follows: 000 = NIU 101 = 101..106 201 = 201..203 301 = 301 401 = 401..405 501 = 501..502 601 = 601..602 701 = 701..706, 801..802, 900..904"""
        return (leftpad(x,3)==leftpad('0',3)) or (leftpad('101',3) <= leftpad(x,3) <= leftpad('106',3)) or (leftpad('201',3) <= leftpad(x,3) <= leftpad('203',3)) or (leftpad(x,3)==leftpad('301',3)) or (leftpad('401',3) <= leftpad(x,3) <= leftpad('405',3)) or (leftpad('501',3) <= leftpad(x,3) <= leftpad('502',3)) or (leftpad('601',3) <= leftpad(x,3) <= leftpad('602',3)) or (leftpad('701',3) <= leftpad(x,3) <= leftpad('702',3)) or (leftpad(x,3)==leftpad('704',3)) or (leftpad(x,3)==leftpad('706',3)) or (leftpad('801',3) <= leftpad(x,3) <= leftpad('802',3)) or (leftpad('900',3) <= leftpad(x,3) <= leftpad('901',3)) or (leftpad('903',3) <= leftpad(x,3) <= leftpad('904',3))
    @classmethod
    def is_valid_TEN(self,x):
        """Tenure"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('4',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_VACS(self,x):
        """Vacancy Status"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('7',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_FINAL_POP(self,x):
        """Population Count"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 99999)
    @classmethod
    def is_valid_HHT(self,x):
        """Household/Family Type"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('7',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_HHT2(self,x):
        """Household/Family Type (NEW)"""
        return (leftpad('0',2) <= leftpad(x,2) <= leftpad('12',2)) or (leftpad(x,2)==leftpad('99',2))
    @classmethod
    def is_valid_NPF(self,x):
        """Number of People in Family"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (x==0) or (2 <= x <= 97) or (x==99)
    @classmethod
    def is_valid_CPLT(self,x):
        """Couple Type"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('4',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_UPART(self,x):
        """Presence and Type of Unmarried Partner Household"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('5',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_MULTG(self,x):
        """Multigenerational Household"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('2',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_HHLDRAGE(self,x):
        """Age of Householder"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (x==0) or (15 <= x <= 115) or (x==999)
    @classmethod
    def is_valid_HHSPAN(self,x):
        """Hispanic or Latino Householder"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('2',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_HHRACE(self,x):
        """Race of Householder"""
        return (leftpad('0',2) <= leftpad(x,2) <= leftpad('63',2)) or (leftpad(x,2)==leftpad('99',2))
    @classmethod
    def is_valid_PAOC(self,x):
        """Presence and Age of Own Children Under 18"""
        return (leftpad('0',1) <= leftpad(x,1) <= leftpad('4',1)) or (leftpad(x,1)==leftpad('9',1))
    @classmethod
    def is_valid_P18(self,x):
        """Number of People Under 18 Years in Household"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 97) or (x==99)
    @classmethod
    def is_valid_P60(self,x):
        """Number of People 60 Years and Over in Household"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 97) or (x==99)
    @classmethod
    def is_valid_P65(self,x):
        """Number of People 65 Years and Over in Household"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 97) or (x==99)
    @classmethod
    def is_valid_P75(self,x):
        """Number of People 75 Years and Over in Household"""
        x = str(x).strip()
        try:
            x = int(x)
        except ValueError:
            return False
        return (0 <= x <= 97) or (x==99)

    @classmethod
    def validate_pipe_delimited(self,x):
        fields = x.split('|')
        if len(fields)!=27: return False
        if is_valid_SCHEMA_TYPE_CODE(fields[1]) == False: return False
        if is_valid_SCHEMA_BUILD_ID(fields[2]) == False: return False
        if is_valid_TABBLKST(fields[3]) == False: return False
        if is_valid_TABBLKCOU(fields[4]) == False: return False
        if is_valid_TABTRACTCE(fields[5]) == False: return False
        if is_valid_TABBLKGRPCE(fields[6]) == False: return False
        if is_valid_TABBLK(fields[7]) == False: return False
        if is_valid_EUID(fields[8]) == False: return False
        if is_valid_RTYPE(fields[9]) == False: return False
        if is_valid_GQTYPE(fields[10]) == False: return False
        if is_valid_TEN(fields[11]) == False: return False
        if is_valid_VACS(fields[12]) == False: return False
        if is_valid_FINAL_POP(fields[13]) == False: return False
        if is_valid_HHT(fields[14]) == False: return False
        if is_valid_HHT2(fields[15]) == False: return False
        if is_valid_NPF(fields[16]) == False: return False
        if is_valid_CPLT(fields[17]) == False: return False
        if is_valid_UPART(fields[18]) == False: return False
        if is_valid_MULTG(fields[19]) == False: return False
        if is_valid_HHLDRAGE(fields[20]) == False: return False
        if is_valid_HHSPAN(fields[21]) == False: return False
        if is_valid_HHRACE(fields[22]) == False: return False
        if is_valid_PAOC(fields[23]) == False: return False
        if is_valid_P18(fields[24]) == False: return False
        if is_valid_P60(fields[25]) == False: return False
        if is_valid_P65(fields[26]) == False: return False
        if is_valid_P75(fields[27]) == False: return False
        return True

class MDF_Unit:
    __slots__ = ['SCHEMA_TYPE_CODE', 'SCHEMA_BUILD_ID', 'TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK', 'EUID', 'RTYPE', 'GQTYPE', 'TEN', 'VACS', 'FINAL_POP', 'HHT', 'HHT2', 'NPF', 'CPLT', 'UPART', 'MULTG', 'HHLDRAGE', 'HHSPAN', 'HHRACE', 'PAOC', 'P18', 'P60', 'P65', 'P75']
    def __repr__(self):
        return 'MDF_Unit<SCHEMA_TYPE_CODE:{},SCHEMA_BUILD_ID:{},TABBLKST:{},TABBLKCOU:{},TABTRACTCE:{},TABBLKGRPCE:{},TABBLK:{},EUID:{},RTYPE:{},GQTYPE:{},TEN:{},VACS:{},FINAL_POP:{},HHT:{},HHT2:{},NPF:{},CPLT:{},UPART:{},MULTG:{},HHLDRAGE:{},HHSPAN:{},HHRACE:{},PAOC:{},P18:{},P60:{},P65:{},P75:{}>'.format(self.SCHEMA_TYPE_CODE,self.SCHEMA_BUILD_ID,self.TABBLKST,self.TABBLKCOU,self.TABTRACTCE,self.TABBLKGRPCE,self.TABBLK,self.EUID,self.RTYPE,self.GQTYPE,self.TEN,self.VACS,self.FINAL_POP,self.HHT,self.HHT2,self.NPF,self.CPLT,self.UPART,self.MULTG,self.HHLDRAGE,self.HHSPAN,self.HHRACE,self.PAOC,self.P18,self.P60,self.P65,self.P75)
    def __init__(self,line=None):
        if line: 
            if '|' in line: 
                self.parse_pipe(line)
            else:
                self.parse_fixed(line)
    def parse_pipe(self,line):
        cols = line.split('|')
        assert len(cols)==27
        self.SCHEMA_TYPE_CODE = cols[0]
        self.SCHEMA_BUILD_ID = cols[1]
        self.TABBLKST = cols[2]
        self.TABBLKCOU = cols[3]
        self.TABTRACTCE = cols[4]
        self.TABBLKGRPCE = cols[5]
        self.TABBLK = cols[6]
        self.EUID = cols[7]
        self.RTYPE = cols[8]
        self.GQTYPE = cols[9]
        self.TEN = cols[10]
        self.VACS = cols[11]
        self.FINAL_POP = cols[12]
        self.HHT = cols[13]
        self.HHT2 = cols[14]
        self.NPF = cols[15]
        self.CPLT = cols[16]
        self.UPART = cols[17]
        self.MULTG = cols[18]
        self.HHLDRAGE = cols[19]
        self.HHSPAN = cols[20]
        self.HHRACE = cols[21]
        self.PAOC = cols[22]
        self.P18 = cols[23]
        self.P60 = cols[24]
        self.P65 = cols[25]
        self.P75 = cols[26]

    def parse_fixed(self,line):
        self.SCHEMA_TYPE_CODE = None   # no position information for SCHEMA_TYPE_CODE
        self.SCHEMA_BUILD_ID = None   # no position information for SCHEMA_BUILD_ID
        self.TABBLKST = None   # no position information for TABBLKST
        self.TABBLKCOU = None   # no position information for TABBLKCOU
        self.TABTRACTCE = None   # no position information for TABTRACTCE
        self.TABBLKGRPCE = None   # no position information for TABBLKGRPCE
        self.TABBLK = None   # no position information for TABBLK
        self.EUID = None   # no position information for EUID
        self.RTYPE = None   # no position information for RTYPE
        self.GQTYPE = None   # no position information for GQTYPE
        self.TEN = None   # no position information for TEN
        self.VACS = None   # no position information for VACS
        self.FINAL_POP = None   # no position information for FINAL_POP
        self.HHT = None   # no position information for HHT
        self.HHT2 = None   # no position information for HHT2
        self.NPF = None   # no position information for NPF
        self.CPLT = None   # no position information for CPLT
        self.UPART = None   # no position information for UPART
        self.MULTG = None   # no position information for MULTG
        self.HHLDRAGE = None   # no position information for HHLDRAGE
        self.HHSPAN = None   # no position information for HHSPAN
        self.HHRACE = None   # no position information for HHRACE
        self.PAOC = None   # no position information for PAOC
        self.P18 = None   # no position information for P18
        self.P60 = None   # no position information for P60
        self.P65 = None   # no position information for P65
        self.P75 = None   # no position information for P75
    def validate(self):
        """Return True if the object data validates"""
        if not MDF_Unit_validator.is_valid_SCHEMA_TYPE_CODE(self.SCHEMA_TYPE_CODE): return False
        if not MDF_Unit_validator.is_valid_SCHEMA_BUILD_ID(self.SCHEMA_BUILD_ID): return False
        if not MDF_Unit_validator.is_valid_TABBLKST(self.TABBLKST): return False
        if not MDF_Unit_validator.is_valid_TABBLKCOU(self.TABBLKCOU): return False
        if not MDF_Unit_validator.is_valid_TABTRACTCE(self.TABTRACTCE): return False
        if not MDF_Unit_validator.is_valid_TABBLKGRPCE(self.TABBLKGRPCE): return False
        if not MDF_Unit_validator.is_valid_TABBLK(self.TABBLK): return False
        if not MDF_Unit_validator.is_valid_EUID(self.EUID): return False
        if not MDF_Unit_validator.is_valid_RTYPE(self.RTYPE): return False
        if not MDF_Unit_validator.is_valid_GQTYPE(self.GQTYPE): return False
        if not MDF_Unit_validator.is_valid_TEN(self.TEN): return False
        if not MDF_Unit_validator.is_valid_VACS(self.VACS): return False
        if not MDF_Unit_validator.is_valid_FINAL_POP(self.FINAL_POP): return False
        if not MDF_Unit_validator.is_valid_HHT(self.HHT): return False
        if not MDF_Unit_validator.is_valid_HHT2(self.HHT2): return False
        if not MDF_Unit_validator.is_valid_NPF(self.NPF): return False
        if not MDF_Unit_validator.is_valid_CPLT(self.CPLT): return False
        if not MDF_Unit_validator.is_valid_UPART(self.UPART): return False
        if not MDF_Unit_validator.is_valid_MULTG(self.MULTG): return False
        if not MDF_Unit_validator.is_valid_HHLDRAGE(self.HHLDRAGE): return False
        if not MDF_Unit_validator.is_valid_HHSPAN(self.HHSPAN): return False
        if not MDF_Unit_validator.is_valid_HHRACE(self.HHRACE): return False
        if not MDF_Unit_validator.is_valid_PAOC(self.PAOC): return False
        if not MDF_Unit_validator.is_valid_P18(self.P18): return False
        if not MDF_Unit_validator.is_valid_P60(self.P60): return False
        if not MDF_Unit_validator.is_valid_P65(self.P65): return False
        if not MDF_Unit_validator.is_valid_P75(self.P75): return False
        return True
    def validate_reason(self):
        reason=[]
        if not MDF_Unit_validator.is_valid_SCHEMA_TYPE_CODE(self.SCHEMA_TYPE_CODE): reason.append('SCHEMA_TYPE_CODE ('+str(self.SCHEMA_TYPE_CODE)+') out of range ()')
        if not MDF_Unit_validator.is_valid_SCHEMA_BUILD_ID(self.SCHEMA_BUILD_ID): reason.append('SCHEMA_BUILD_ID ('+str(self.SCHEMA_BUILD_ID)+') out of range ()')
        if not MDF_Unit_validator.is_valid_TABBLKST(self.TABBLKST): reason.append('TABBLKST ('+str(self.TABBLKST)+') out of range (1-2, 4-6, 8-13, 15-42, 44-51, 53-56)')
        if not MDF_Unit_validator.is_valid_TABBLKCOU(self.TABBLKCOU): reason.append('TABBLKCOU ('+str(self.TABBLKCOU)+') out of range (1-840)')
        if not MDF_Unit_validator.is_valid_TABTRACTCE(self.TABTRACTCE): reason.append('TABTRACTCE ('+str(self.TABTRACTCE)+') out of range (100-998999)')
        if not MDF_Unit_validator.is_valid_TABBLKGRPCE(self.TABBLKGRPCE): reason.append('TABBLKGRPCE ('+str(self.TABBLKGRPCE)+') out of range (0-9)')
        if not MDF_Unit_validator.is_valid_TABBLK(self.TABBLK): reason.append('TABBLK ('+str(self.TABBLK)+') out of range (1-9999)')
        if not MDF_Unit_validator.is_valid_EUID(self.EUID): reason.append('EUID ('+str(self.EUID)+') out of range (0-999999999)')
        if not MDF_Unit_validator.is_valid_RTYPE(self.RTYPE): reason.append('RTYPE ('+str(self.RTYPE)+') out of range (2-2, 4-4)')
        if not MDF_Unit_validator.is_valid_GQTYPE(self.GQTYPE): reason.append('GQTYPE ('+str(self.GQTYPE)+') out of range (0-0, 101-106, 201-203, 301-301, 401-405, 501-502, 601-602, 701-702, 704-704, 706-706, 801-802, 900-901, 903-904)')
        if not MDF_Unit_validator.is_valid_TEN(self.TEN): reason.append('TEN ('+str(self.TEN)+') out of range (0-4, 9-9)')
        if not MDF_Unit_validator.is_valid_VACS(self.VACS): reason.append('VACS ('+str(self.VACS)+') out of range (0-7, 9-9)')
        if not MDF_Unit_validator.is_valid_FINAL_POP(self.FINAL_POP): reason.append('FINAL_POP ('+str(self.FINAL_POP)+') out of range (0-99999)')
        if not MDF_Unit_validator.is_valid_HHT(self.HHT): reason.append('HHT ('+str(self.HHT)+') out of range (0-7, 9-9)')
        if not MDF_Unit_validator.is_valid_HHT2(self.HHT2): reason.append('HHT2 ('+str(self.HHT2)+') out of range (0-12, 99-99)')
        if not MDF_Unit_validator.is_valid_NPF(self.NPF): reason.append('NPF ('+str(self.NPF)+') out of range (0-0, 2-97, 99-99)')
        if not MDF_Unit_validator.is_valid_CPLT(self.CPLT): reason.append('CPLT ('+str(self.CPLT)+') out of range (0-4, 9-9)')
        if not MDF_Unit_validator.is_valid_UPART(self.UPART): reason.append('UPART ('+str(self.UPART)+') out of range (0-5, 9-9)')
        if not MDF_Unit_validator.is_valid_MULTG(self.MULTG): reason.append('MULTG ('+str(self.MULTG)+') out of range (0-2, 9-9)')
        if not MDF_Unit_validator.is_valid_HHLDRAGE(self.HHLDRAGE): reason.append('HHLDRAGE ('+str(self.HHLDRAGE)+') out of range (0-0, 15-115, 999-999)')
        if not MDF_Unit_validator.is_valid_HHSPAN(self.HHSPAN): reason.append('HHSPAN ('+str(self.HHSPAN)+') out of range (0-2, 9-9)')
        if not MDF_Unit_validator.is_valid_HHRACE(self.HHRACE): reason.append('HHRACE ('+str(self.HHRACE)+') out of range (0-63, 99-99)')
        if not MDF_Unit_validator.is_valid_PAOC(self.PAOC): reason.append('PAOC ('+str(self.PAOC)+') out of range (0-4, 9-9)')
        if not MDF_Unit_validator.is_valid_P18(self.P18): reason.append('P18 ('+str(self.P18)+') out of range (0-97, 99-99)')
        if not MDF_Unit_validator.is_valid_P60(self.P60): reason.append('P60 ('+str(self.P60)+') out of range (0-97, 99-99)')
        if not MDF_Unit_validator.is_valid_P65(self.P65): reason.append('P65 ('+str(self.P65)+') out of range (0-97, 99-99)')
        if not MDF_Unit_validator.is_valid_P75(self.P75): reason.append('P75 ('+str(self.P75)+') out of range (0-97, 99-99)')
        return ', '.join(reason)


