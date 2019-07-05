General Purpose FFT (Fast Fourier/Cosine/Sine Transform) Package

Description:
    A package to calculate Discrete Fourier/Cosine/Sine Transforms of 
    1-dimensional sequences of length 2^N.

Files:
    fft4g.c    : FFT Package in C       - Fast Version   I   (radix 4,2)
    fft4g.f    : FFT Package in Fortran - Fast Version   I   (radix 4,2)
    fft4g_h.c  : FFT Package in C       - Simple Version I   (radix 4,2)
    fft8g.c    : FFT Package in C       - Fast Version   II  (radix 8,4,2)
    fft8g.f    : FFT Package in Fortran - Fast Version   II  (radix 8,4,2)
    fft8g_h.c  : FFT Package in C       - Simple Version II  (radix 8,4,2)
    fftsg.c    : FFT Package in C       - Fast Version   III (Split-Radix)
    fftsg.f    : FFT Package in Fortran - Fast Version   III (Split-Radix)
    fftsg_h.c  : FFT Package in C       - Simple Version III (Split-Radix)
    readme.txt : Readme File
    sample1/   : Test Directory
        Makefile    : for gcc, cc
        Makefile.f77: for Fortran
        testxg.c    : Test Program for "fft*g.c"
        testxg.f    : Test Program for "fft*g.f"
        testxg_h.c  : Test Program for "fft*g_h.c"
    sample2/   : Benchmark Directory
        Makefile    : for gcc, cc
        Makefile.pth: POSIX Thread version
        pi_fft.c    : PI(= 3.1415926535897932384626...) Calculation Program
                      for a Benchmark Test for "fft*g.c"

Difference of the Files:
    C and Fortran versions are equal and 
    the same routines are in each version.
    "fft4g*.*" are optimized for most machines.
    "fft8g*.*" are fast on the UltraSPARC.
    "fftsg*.*" are optimized for the machines that 
    have the multi-level (L1,L2,etc) cache.
    The simple versions "fft*g_h.c" use no work area, but 
    the fast versions "fft*g.*" use work areas.
    The fast versions "fft*g.*" have the same specification.

Routines in the Package:
    cdft: Complex Discrete Fourier Transform
    rdft: Real Discrete Fourier Transform
    ddct: Discrete Cosine Transform
    ddst: Discrete Sine Transform
    dfct: Cosine Transform of RDFT (Real Symmetric DFT)
    dfst: Sine Transform of RDFT (Real Anti-symmetric DFT)

Usage:
    Please refer to the comments in the "fft**.*" file which 
    you want to use. Brief explanations are in the block 
    comments of each package. The examples are also given in 
    the test programs.

Method:
    -------- cdft --------
    fft4g*.*, fft8g*.*:
        A method of in-place, radix 2^M, Sande-Tukey (decimation in 
        frequency). Index of the butterfly loop is in bit 
        reverse order to keep continuous memory access.
    fftsg*.*:
        A method of in-place, Split-Radix, recursive fast 
        algorithm.
    -------- rdft --------
    A method with a following butterfly operation appended to "cdft".
    In forward transform :
        A[k] = sum_j=0^n-1 a[j]*W(n)^(j*k), 0<=k<=n/2, 
            W(n) = exp(2*pi*i/n), 
    this routine makes an array x[] :
        x[j] = a[2*j] + i*a[2*j+1], 0<=j<n/2
    and calls "cdft" of length n/2 :
        X[k] = sum_j=0^n/2-1 x[j] * W(n/2)^(j*k), 0<=k<n.
    The result A[k] are :
        A[k]     = X[k]     - (1+i*W(n)^k)/2 * (X[k]-conjg(X[n/2-k])), 
        A[n/2-k] = X[n/2-k] + 
                        conjg((1+i*W(n)^k)/2 * (X[k]-conjg(X[n/2-k]))), 
            0<=k<=n/2
        (notes: conjg() is a complex conjugate, X[n/2]=X[0]).
    -------- ddct --------
    A method with a following butterfly operation appended to "rdft".
    In backward transform :
        C[k] = sum_j=0^n-1 a[j]*cos(pi*j*(k+1/2)/n), 0<=k<n, 
    this routine makes an array r[] :
        r[0] = a[0], 
        r[j]   = Re((a[j] - i*a[n-j]) * W(4*n)^j*(1+i)/2), 
        r[n-j] = Im((a[j] - i*a[n-j]) * W(4*n)^j*(1+i)/2), 
            0<j<=n/2
    and calls "rdft" of length n :
        A[k] = sum_j=0^n-1 r[j]*W(n)^(j*k), 0<=k<=n/2, 
            W(n) = exp(2*pi*i/n).
    The result C[k] are :
        C[2*k]   =  Re(A[k] * (1-i)), 
        C[2*k-1] = -Im(A[k] * (1-i)).
    -------- ddst --------
    A method with a following butterfly operation appended to "rdft".
    In backward transform :
        S[k] = sum_j=1^n A[j]*sin(pi*j*(k+1/2)/n), 0<=k<n, 
    this routine makes an array r[] :
        r[0] = a[0], 
        r[j]   = Im((a[n-j] - i*a[j]) * W(4*n)^j*(1+i)/2), 
        r[n-j] = Re((a[n-j] - i*a[j]) * W(4*n)^j*(1+i)/2), 
            0<j<=n/2
    and calls "rdft" of length n :
        A[k] = sum_j=0^n-1 r[j]*W(n)^(j*k), 0<=k<=n/2, 
            W(n) = exp(2*pi*i/n).
    The result S[k] are :
        S[2*k]   =  Re(A[k] * (1+i)), 
        S[2*k-1] = -Im(A[k] * (1+i)).
    -------- dfct --------
    A method to split into "dfct" and "ddct" of half length.
    The transform :
        C[k] = sum_j=0^n a[j]*cos(pi*j*k/n), 0<=k<=n
    is divided into :
        C[2*k]   = sum'_j=0^n/2  (a[j]+a[n-j])*cos(pi*j*k/(n/2)), 
        C[2*k+1] = sum_j=0^n/2-1 (a[j]-a[n-j])*cos(pi*j*(k+1/2)/(n/2))
        (sum' is a summation whose last term multiplies 1/2).
    This routine uses "ddct" recursively.
    To keep the in-place operation, the data in fft*g_h.*
    are sorted in bit reversal order.
    -------- dfst --------
    A method to split into "dfst" and "ddst" of half length.
    The transform :
        S[k] = sum_j=1^n-1 a[j]*sin(pi*j*k/n), 0<k<n
    is divided into :
        S[2*k]   = sum_j=1^n/2-1 (a[j]-a[n-j])*sin(pi*j*k/(n/2)), 
        S[2*k+1] = sum'_j=1^n/2  (a[j]+a[n-j])*sin(pi*j*(k+1/2)/(n/2))
        (sum' is a summation whose last term multiplies 1/2).
    This routine uses "ddst" recursively.
    To keep the in-place operation, the data in fft*g_h.*
    are sorted in bit reversal order.

Reference:
    * Masatake MORI, Makoto NATORI, Tatuo TORII: Suchikeisan, 
      Iwanamikouzajyouhoukagaku18, Iwanami, 1982 (Japanese)
    * Henri J. Nussbaumer: Fast Fourier Transform and Convolution 
      Algorithms, Springer Verlag, 1982
    * C. S. Burrus, Notes on the FFT (with large FFT paper list)
      http://www-dsp.rice.edu/research/fft/fftnote.asc

Copyright:
    Copyright(C) 1996-2001 Takuya OOURA
    email: ooura@mmm.t.u-tokyo.ac.jp
    download: http://momonga.t.u-tokyo.ac.jp/~ooura/fft.html
    You may use, copy, modify this code for any purpose and 
    without fee. You may distribute this ORIGINAL package.

History:
    ...
    Dec. 1995  : Edit the General Purpose FFT
    Mar. 1996  : Change the specification
    Jun. 1996  : Change the method of trigonometric function table
    Sep. 1996  : Modify the documents
    Feb. 1997  : Change the butterfly loops
    Dec. 1997  : Modify the documents
    Dec. 1997  : Add "fft4g.*"
    Jul. 1998  : Fix some bugs in the documents
    Jul. 1998  : Add "fft8g.*" and delete "fft4f.*"
    Jul. 1998  : Add a benchmark program "pi_fft.c"
    Jul. 1999  : Add a simple version "fft*g_h.c"
    Jul. 1999  : Add a Split-Radix FFT package "fftsg*.c"
    Sep. 1999  : Reduce the memory operation (minor optimization)
    Oct. 1999  : Change the butterfly structure of "fftsg*.c"
    Oct. 1999  : Save the code size
    Sep. 2001  : Add "fftsg.f"
    Sep. 2001  : Add Pthread & Win32thread routines to "fftsg*.c"
    Dec. 2006  : Fix a minor bug in "fftsg.f"

