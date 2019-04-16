class Const:
    @property
    def GUROBI(self):
        return "GUROBI"

Const = Const() #create singleton class so getters and setters work
    
