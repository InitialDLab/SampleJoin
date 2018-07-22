#include <cstdio>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <cstring>
using namespace std;

void mMultiply(double *A, double *B, double *C, int m) {
    double s;
    for (int i = 0; i < m; ++i) {
        for (int j = 0; j < m; ++j) {
            s = 0.;
            for (int k = 0; k < m; ++k) {
                s += A[i*m+k] * B[k*m+j];
                C[i*m+j] = s;
            }
        }
    }
}

void mPower(double *A, int eA, double *V, int *eV, int m, int n) {
    double *B;
    if (n == 1) {
        memcpy(V, A, sizeof(double) * m * m);
        *eV = eA;
        return ;
    }
    mPower(A, eA, V, eV, m, n / 2);
    B = new double[m * m];
    mMultiply(V, V, B, m);
    int eB = 2 * (*eV);
    if (n & 1) {
        mMultiply(A, B, V, m);
        *eV = eA + eB;
    } else {
        memcpy(V, B, sizeof(double) * m * m);
        *eV = eB;
    }
    if (V[(m/2) * m + (m/2)] > 1e140) {
        for (int i = 0; i < m * m; ++i) {
            V[i] = V[i] * 1e-140;
            *eV += 140;
        }
    }
    delete []B;
}

double K(int n, double d) {
    int k, m, i, j, g, eH, eQ;
    double h, s, *H, *Q;
    // omit next 4 lines if > 7 digit accuracy is required in the right tail
    s = d * d * n;
    if (s > 7.24 || (s > 3.76 && n > 99)) {
         return 1 - 2 * exp(-(2.000071 + .331 / sqrt(n) + 1.409 / n) * s);
    }
    k = (int) (n * d) + 1;
    m = 2 * k - 1;
    h = k - n * d;
    H = new double[m * m];
    Q = new double[m * m];
    for (i = 0; i < m; ++i) {
        for (j = 0; j < m; ++j) {
            if (i - j + 1 < 0) {
                H[i * m + j] = 0;
            } else {
                H[i * m + j] = 1;
            }
        }
    }
    for (i = 0; i < m; ++i) {
        H[i*m] -= pow(h, i+1);
        H[(m-1) * m + i] -= pow(h, m-i);
    }
    H[(m-1) * m] += (2*h-1>0 ? pow(2 * h - 1, m) : 0);
    for (i = 0; i < m; ++i) {
        for (j = 0; j < m; ++j) {
            if (i - j + 1 > 0) {
                for (g = 1; g <= i - j + 1; ++g) {
                    H[i*m+j] /= g;
                }
            }
        }
    }
    eH = 0;
    mPower(H, eH, Q, &eQ, m, n);
    s = Q[(k-1) * m + k - 1];
    for (i = 1; i <= n; ++i) {
        s = s * i / n;
        if (s < 1e-140) {
            s *= 1e140;
            eQ -= 140;
        }
    }
    s *= pow(10., eQ);
    delete []H;
    delete []Q;
    return s;
}

int main(int argc, char *argv[]) {

    if (argc < 3) {
        cout << "usage: " << argv[0] << " <n> <d>" << endl;
        return 1;
    }
    
    int n = atoi(argv[1]);
    double d = atof(argv[2]);

    cout << K(n, d) << endl;

    return 0;
}
