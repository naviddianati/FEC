import igraph as ig
from numpy import sin,cos,abs
from matplotlib import cm 


class mplPalette(ig.Palette):
    def __init__(self,n,name='Set1', reverse = False):
        ig.Palette.__init__(self,n)
        self.n = n
        self.matplotlib_palette = cm.get_cmap(name)
        self.reverse = reverse
    # Accepts parameter vin [0,colornum]
    def _get(self,v):
        if self.reverse:
            x=1.-1.0*v/self.n
        else:
            x=1.0*v/self.n
        color_tmp = self.matplotlib_palette(x)
        color = (color_tmp[0],color_tmp[1],color_tmp[2])
    
        return color



class gnuplotPalette1(ig.Palette):

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