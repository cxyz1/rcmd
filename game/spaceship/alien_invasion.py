import sys
import pygame
from game.spaceship.settings import Settings
from game.spaceship.ship import Ship

def run_game():
    pygame.display.set_caption("Alien Invasion")

    pygame.init()
    ai_settings = Settings()

    screen = pygame.display.set_mode((
        ai_settings.screen_width,ai_settings.screen_height))
    bg_color = ai_settings.bg_color

    ship = Ship(screen)

    while True:
        screen.fill(bg_color)
        ship.blitme()

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                sys.exit()


        pygame.display.flip()

run_game()
