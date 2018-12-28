package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import java.util.ArrayList;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determine the number of localPrimes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of localPrimes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor sieveActorActor = new SieveActorActor();
        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                sieveActorActor.send(i);
            }
            sieveActorActor.send(0);
        });

        int totalPrimes = 1;
        SieveActorActor currentActor = sieveActorActor;
        while (currentActor != null) {
            totalPrimes += currentActor.getLocalPrimesSize();
            currentActor = currentActor.getNextActor();
        }

        return totalPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        private static final int MAX_ALLOWED_LOCAL_PRIMES = 1000;
        private SieveActorActor nextActor;
        private ArrayList<Integer> localPrimes;

        SieveActorActor() {
            this.localPrimes = new ArrayList<>();
            this.nextActor = null;
        }

        public SieveActorActor getNextActor() {
            return nextActor;
        }

        public int getLocalPrimesSize() {
            return localPrimes.size();
        }

        /**
         * Process a single message sent to this actor.
         * <p>
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            final int candidate = (int)msg;

            // invalid, terminate children and exit
            if (candidate < 1) {
                if (nextActor != null) {
                    nextActor.send(msg);
                }
                return;
            }

            // if not prime, ignore
            if (!isLocallyPrime(candidate)) {
                return;
            }

            // if we can add more localPrimes, do so
            if (localPrimes.size() < MAX_ALLOWED_LOCAL_PRIMES) {
                localPrimes.add(candidate);
                return;
            }

            // if we can't, send the message down the chain
            if (nextActor == null) {
                nextActor = new SieveActorActor();
            }

            nextActor.send(msg);
        }

        private boolean isLocallyPrime(final int num) {
            for (int prime : localPrimes) {
                if (num % prime == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
