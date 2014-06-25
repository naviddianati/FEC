#! /usr/bin/gnuplot

set term pdf enhanced rounded color size 20,9
set output "likelihoods.pdf"
set xtics rotate by -90 offset character 0,0 font ',36' 
set style fill solid 0.6 border  lc rgb "#55000000"
#set xtics mirror
set bmargin 19
set grid ytics ls 1 lc rgb "#ee000000"
set ytics rotate by -90
unset key
unset colorbox
set  yrange [:10]

set style textbox opaque
set label "Signal: weak" at graph 0.18,0.9 front  font "Arial Bold,28"
set label "Signal: moderate" at graph 0.43,0.9 front font "Arial Bold,36"
set label "Signal: strong" at graph 0.70,0.9  front font "Arial Bold,48"


get_threshold(N)=1.0/N*(N-1)/N

threshold = get_threshold(3)

set logscale y
plot (threshold) with lines lw 10 lc rgb "#5500ff00",\
    "likelihoods.txt" using 1:($2):(0.7):( $2 >=threshold+0.0001 ? $2/3.0: ($2 < threshold ? $2/3:0)):xtic(4) with boxes lw 4 palette notitle

set output

system "pdftk ./likelihoods.pdf cat 1W output likelihoods-rotated.pdf