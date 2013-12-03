import igraph as ig
from numpy import sin,cos,abs
class MyPalette(ig.Palette):

    def __init__(self,n):
        ig.Palette.__init__(self,n)
        self.n = n

    # Accepts parameter vin [0,colornum]
    def _get(self,v):
        x=1.0*v/self.n
        return(abs(2*x - 0.5),sin(3.1415*x),cos(3.1415/2*x),1)
    

    # Accecpts parameter v in [0,1]
    def _getRGB255(self,v):
        x=v
        return(
            int(abs(2*x - 0.5)*self.n),
            int(sin(3.1415*x)*self.n),
            int(cos(3.1415/2*x)*self.n)
        )